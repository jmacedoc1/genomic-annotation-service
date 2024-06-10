# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
__editor__ = 'Josemaria Macedo <josemaria@uchicago.edu'

# Resources:
# - https://www.geeksforgeeks.org/convert-python-datetime-to-epoch/
# - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
# - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html

import os
import sys
import boto3
import json
import time
from datetime import datetime, timedelta
import shutil

# Path variables that are used in different services
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_ID_DIR = os.path.join(CURRENT_DIR, "job_id")

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')
sqs_url = config.get('aws', 'SQS_GLACIER_URL')
REGION = config.get('aws', 'AwsRegionName')
GLACIER_VAULT_NAME = config.get('aws', 'GLACIER_VAULT_NAME')
ANNOTATIONS_TABLE = config.get('aws', 'ANNOTATIONS_TABLE')

def create_client(service_type="s3", region="us-east-1"):
    """
    Create AWS client of specific service and region.

    Inputs:
        service_type (`str`): AWS service type, it can be either 'ec2' or 's3'.
            Default is set to 's3'
        region (`str`): client's region. Default is set to 'us-east-1'.
    
    Returns (`Client`): AWS client of type 'service_type'.
    """

    return boto3.client(service_type, region_name=region)

S3_CLIENT = create_client(service_type="s3", region=REGION)

queue = boto3.resource("sqs", region_name = "us-east-1").Queue(sqs_url)

while True:
    messages = queue.receive_messages(WaitTimeSeconds=10)

    for message in messages:
        body = json.loads(message.body)
        message_str = body["Message"]
        print("Message: ", message_str)

        if message_str:
            # Convert single quotes in message string to double quotes
            # so we can transform to a dict
            message_str = message_str.replace("'", "\"")
            try:
                message_dict = json.loads(message_str)
                results_bucket = message_dict["s3_results_bucket"]
                results_key = message_dict["key_annot"]
                job_id = message_dict["job_id"]
                file_name = message_dict["file_annot"]
                user_id = message_dict["user_id"]
                user_role = message_dict["user_role"]
                complete_time = message_dict["complete_time"]
            except Exception as e:
                print({"code": 500,
                "error": f"Error converting message to dictionary and/or extracting keys.",
                "message": str(e)})

            # We check if it's time to freeze a job in Glacier's vault
            complete_datetime = datetime.fromtimestamp(complete_time)
            complete_datetime_limit = complete_datetime + timedelta(minutes=5)
            free_user_time_limit = int(complete_datetime_limit.timestamp())
            current_time = int(time.time())

            if current_time >= free_user_time_limit:
                
                response = S3_CLIENT.get_object(Bucket=results_bucket, Key=results_key)
                s3_key_result_content = response['Body'].read()
                
                # Upload file to glacier vault
                glacier_client = boto3.client('glacier', region_name=REGION)

                response = glacier_client.upload_archive(
                    vaultName=GLACIER_VAULT_NAME,
                    body=s3_key_result_content
                )

                archive_id = response['archiveId']

                # Persist results_file_archive_id in DynamoDB annotations table 
                key = {"job_id": job_id}

                try:
                    dynamo = boto3.resource('dynamodb')
                    table = dynamo.Table(ANNOTATIONS_TABLE)
                except Exception as e:
                    print(f"Error when trying to create Dynamo DB object. Message: {e}")

                update_expression = 'SET results_file_archive_id = :archive_id'
                expression_attribute_values = {':archive_id': archive_id}

                try:
                    response = table.update_item(
                    Key=key,
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values
                    )
                except Exception as e:
                    print(f"Error when trying to update Dynamo DB object. Message: {e}")
                
                # Delete file object from s3 results bucket
                try:
                    response = S3_CLIENT.delete_object(
                        Bucket=results_bucket,
                        Key=results_key
                    )
                    print(f"Object '{results_key}' deleted successfully from '{results_bucket}'.")
                except Exception as e:
                    print(f"Error when trying to delete object. Message: {e}")

                # Delete message from queue 
                message.delete()
                print("Deleted message.")

### EOF