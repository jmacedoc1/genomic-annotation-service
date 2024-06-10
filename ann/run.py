# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
__edited__ = 'Josemaria Macedo <josemaria@uchicago.edu'

import sys
import time
import driver
import boto3
import shutil
import configparser

S3_CLIENT = boto3.client("s3", region_name="us-east-1")

# Load the .ini file
try:
    config = configparser.ConfigParser()
    config.read('ann_config.ini')
except Exception as e:
   print(f"Error when trying to load 'ann_config.ini' file. Message: {e}")

# Get variables from .ini file
try:
    aws_s3_key_prefix = config.get('aws', 'AWS_S3_KEY_PREFIX')
    s3_results_bucket = config.get('aws', 'S3_RESULTS_BUCKET')
    my_table = config.get('aws', 'ANNOTATIONS_TABLE')
    topic_arn = config.get('aws', 'AWS_SNS_JOB_COMPLETE_TOPIC')
    glacier_sns_topic = config.get('aws', 'AWS_SNS_GLACIER_TOPIC')
except Exception as e:
    print(f"Error when trying to get variables from 'ann_config.ini' file. Message: {e}")

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

# Resources:
# - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
# - https://medium.com/@victor.perez.berruezo/upload-a-file-to-s3-using-boto3-python3-lib-25f22e31c993
# - Code from Lecture 4 "dynamo_writer.py" by Lionel Barrow
# - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html#

if __name__ == '__main__':
   # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        with Timer():
            driver.run(file_path, 'vcf')
        
        file_path_split = file_path.split("/")
        file = file_path_split[-1]
        job_id = file_path_split[-2]
        user_id = file_path_split[-3]

        # Upload the annotation results file
        file_annot = file.replace(".vcf", ".annot.vcf")

        # Get the AWS key prefix from the .ini file
        key_prefix = aws_s3_key_prefix + user_id + "/"
        key_annot = key_prefix + job_id + "~" + file_annot
        path_annot = file_path.replace(file, file_annot)

        try:
          response = S3_CLIENT.put_object(
             Body=open(path_annot, "rb"),
             Bucket=s3_results_bucket,
             Key=key_annot,
             )

        except Exception as e:
           print(f"Error when trying to put ANNOTATION file in s3 bucket. Message: {e}")
          
        # Upload the log file
        file_log = file + ".count.log"
        key_log = key_prefix + job_id + "~" + file_log
        path_log = file_path.replace(file, file_log)

        try:
          response = S3_CLIENT.put_object(
             Body=open(path_log, "rb"),
             Bucket=s3_results_bucket,
             Key=key_log,
             )
        except Exception as e:
           print(f"Error when trying to put LOG file in s3 bucket. Message: {e}")

        # Update database status to "COMPLETED"
        complete_time = int(time.time())
        job_status = "COMPLETED"
        update_expression = 'SET job_status = :new_status, s3_results_bucket = :results_bucket,\
                              s3_key_result_file = :result_file, s3_key_log_file = :log_file,\
                                complete_time = :complete_time'
        expression_attribute_values = {':new_status': 'COMPLETED',
                                       ':results_bucket': s3_results_bucket,
                                       ':result_file': key_annot, ':log_file': key_log,
                                       ':complete_time': complete_time}
        key = {"job_id": job_id}

        try:
          dynamo = boto3.resource('dynamodb')
          table = dynamo.Table(my_table)
        except Exception as e:
           print(f"Error when trying to create Dynamo DB object. Message: {e}")

        try:
          response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
            )
        except Exception as e:
           print(f"Error when trying to update Dynamo DB object. Message: {e}")
        
        # Delete local job files 
        job_id_path = file_path.replace(file, "")
  
        try:
           shutil.rmtree(job_id_path)
           print(f"Directory '{job_id_path}' and its file were deleted successfully.")
        except Exception as e:
           print(f"Error deleting directory {job_id_path}. Message: {e}")
        
        # Send notificaton to SNS results topic 
        try:
           client = boto3.client('sns')
        except Exception as e:
             print({"code": 500,
                    "error": "Error trying to create SNS client.",
                    "message": str(e)})
         
        try:
           item_key = {"job_id": job_id}
           response = table.get_item(Key=item_key)
           email = response["Item"]["email"]
           user_role = response["Item"]["user_role"]
        except Exception as e:
           print(f"Email or user role wasn't extracted correctly for Dynamo table. Error: {e}")

        
        message_dict = {"job_id": job_id, "user_id": user_id, "file": file, "email": email}
        message = str(message_dict)
    
        try:
            response = client.publish(
            TopicArn=topic_arn,
            Message=message
            )
            message_id = response["MessageId"]
        except Exception as e:
           print({"code": 500,
                  "error": "Error trying to publish message to SNS client.",
                  "message": str(e),
                  "topic_arn": topic_arn,
                  "sns_message": message})

        # Send message to glacier SNS topic so we can archive results annotation file
        if user_role == "free_user":
            message_dict_glacier = {"job_id": job_id, "user_id": user_id, "user_role": user_role,
                                    "complete_time": complete_time, "file_annot": file_annot,
                                    "s3_results_bucket": s3_results_bucket, "key_annot": key_annot}
            message_glacier = str(message_dict_glacier)
            
            try:
               response = client.publish(
                  TopicArn=glacier_sns_topic,
                  Message=message_glacier
                  )
               message_id = response["MessageId"]
            except Exception as e:
               print({"code": 500,
                        "error": "Error trying to publish message to SNS client.",
                        "message": str(e),
                        "glacier_sns_topic": glacier_sns_topic,
                        "sns_message": message_glacier})

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF