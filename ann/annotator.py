import subprocess
import os
import boto3
import json
import configparser

# Path variables that are used in different services
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_ID_DIR = os.path.join(CURRENT_DIR, "job_id")
# ANNTOOLS_PATH = os.path.join(CURRENT_DIR, "ann")
FILE_EXT_DICT = {"annotation": ".annot.vcf", "count": ".count.log"}
REGION = "us-east-1"

# Load the .ini file
try:
    config = configparser.ConfigParser()
    config.read('ann_config.ini')
except Exception as e:
   print(f"Error when trying to load 'ann_config.ini' file. Message: {e}")

# Get variables from .ini file
try:
    sqs_url = config.get('aws', 'SQS_URL')
except Exception as e:
    print(f"Error when trying to get variables from 'ann_config.ini' file. Message: {e}")

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

# Create a SQS Queue object
# sqs_url = "https://sqs.us-east-1.amazonaws.com/659248683008/josemaria_job_requests"
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
                bucket = message_dict["s3_inputs_bucket"]
                key = message_dict["s3_key_input_file"]
                job_id = message_dict["job_id"]
                file_name = message_dict["input_file_name"]
                user_id = message_dict["user_id"]
            except Exception as e:
                print({"code": 500,
                "error": f"Error converting message to dictionary and/or extracting keys.",
                "message": str(e)})

            # Create "job_id" directory if it doesn't exist
            if "job_id" not in os.listdir(CURRENT_DIR):
                try:
                    os.mkdir(JOB_ID_DIR)
                except Exception as e:
                    print({"code": 500,
                            "error":
                            f"Error trying to create 'job_id' directory with path \
                                '{JOB_ID_DIR}'.",
                            "message": str(e)})         
            
            # We create a directory for the job_id inside the "job_id" directory
            job_id_path = os.path.join(JOB_ID_DIR, user_id, job_id)
            try:
                os.makedirs(job_id_path)
            except Exception as e:
                print({"code": 500,
                            "error":
                            f"Error trying to create '{job_id}' directory with path \
                                '{job_id_path}'.",
                            "message": str(e)})

            # Download s3 file object to instance's directory
            file_path = os.path.join(job_id_path, file_name)
            try:
                with open(file_path, 'wb') as f:
                    S3_CLIENT.download_fileobj(bucket, key, f)
            except Exception as e:
                print({"code": 500,
                            "error":
                            f"Error downloading file to local instance's path \
                                '{file_path}'.",
                            "message": str(e)})
                
            # Run subprocess to run annotation
            try:
                run_file = "run.py"
                job = subprocess.Popen(["python", run_file, file_path], cwd = CURRENT_DIR)
                print(f"Launched annotation job '{job_id}'.")
                message.delete()
                print("Deleted message.")
            except Exception as e:
                code = 500
                result = {"code": code, "error": "Error running subprocess with Popen.",
                          "run_file": run_file,
                          "file_path": file_path,
                          "message": str(e)}
                print(result, code, {"Status code": code})

            # Update job status to 'RUNNING' only if it was 'PENDING' before
            try:
                dynamo = boto3.resource('dynamodb')
                table_name = "josemaria_annotations"
                table = dynamo.Table(table_name)
            except Exception as e:
                print({"code": 500,
                       "error": f"Error trying to create dynamo table '{table_name}'.",
                       "message": str(e)})
            
            job_status = "RUNNING"
            update_expression = 'SET job_status = :new_status'
            condition_expression = 'job_status = :pending'
            expression_attribute_values = {':new_status': job_status, ':pending': 'PENDING'}
            key = {"job_id": job_id}

            try:
                response = table.update_item(
                    Key=key,
                    UpdateExpression=update_expression,
                    ConditionExpression=condition_expression,
                    ExpressionAttributeValues=expression_attribute_values
                    )
            except Exception as e:
                print(f"Error when trying to update Dynamo DB object. Message: {e}")
    
    print("Done with loop\n")