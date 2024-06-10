# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')
REGION = config.get('aws', 'AwsRegionName')

# Add utility code here
sqs_url = config.get('aws', 'SQS_THAW_URL')
queue = boto3.resource("sqs", region_name = "us-east-1").Queue(sqs_url)

# TODO: The restore and thawing process was still being tested and doesn't seem it
# was sucessful when submitted

while True:
    messages = queue.receive_messages(WaitTimeSeconds=10)

    for message in messages:
        body = json.loads(message.body)
        message_str = body["Message"]
        print("Message:\n", message_str)

        if message_str:
            # Convert single quotes in message string to double quotes
            # so we can transform to a dict
            message_str = message_str.replace("'", "\"")
            try:
                message_dict = json.loads(message_str)
                results_file_archive_id = message_dict["ArchiveId"]
                retrieval_job_id = message_dict["JobId"]
                s3_key_result_file = message_dict["JobDescription"]
            except Exception as e:
                print({"code": 500,
                "error": f"Error converting message to dictionary and/or extracting keys.",
                "message": str(e)})
        
        # Check if retrieval job is ready to get job output
        GLACIER_VAULT_NAME = config.get('aws', 'GLACIER_VAULT_NAME')
        glacier = boto3.client('glacier', region_name=REGION)

        response = glacier.describe_job(
            vaultName=GLACIER_VAULT_NAME,
            jobId=retrieval_job_id
        )

        job_status = response['StatusCode']

        # If retrieval job finished (job status is "Succeeded") we save the job output
        if job_status == 'Succeeded':
            job_output = glacier.get_job_output(accountId='-', vaultName=GLACIER_VAULT_NAME, jobId=retrieval_job_id)
            s3_results_file_content = job_output["body"].read()

            #Upload job output to s3 results bucket
            s3_client = boto3.client('s3', region_name=REGION)
            AWS_S3_RESULTS_BUCKET = config.get('aws', 'AWS_S3_RESULTS_BUCKET')

            try:
                s3_client.upload_fileobj(s3_results_file_content, AWS_S3_RESULTS_BUCKET, s3_key_result_file)
            except Exception as e:
                print("Error uploading file object ")
            
            # Delete message from queue
            message.delete()
            print("Deleted message.")


### EOF