# restore.py
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
config.read('restore_config.ini')
sqs_url = config.get('aws', 'SQS_RESTORE_URL')
REGION = config.get('aws', 'AwsRegionName')
GLACIER_VAULT_NAME = config.get('aws', 'GLACIER_VAULT_NAME')
ANNOTATIONS_TABLE = config.get('aws', 'ANNOTATIONS_TABLE')
AWS_SNS_THAW_TOPIC = config.get('aws', 'AWS_SNS_THAW_TOPIC')

S3_CLIENT = boto3.client("s3", region_name=REGION)

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
                results_file_archive_id = message_dict["results_file_archive_id"]
                job_id = message_dict["job_id"]
                s3_key_result_file = message_dict["s3_key_result_file"]
            except Exception as e:
                print({"code": 500,
                "error": f"Error converting message to dictionary and/or extracting keys.",
                "message": str(e)})

        # Get restore job id
        # Try expedited version first
        glacier = boto3.client('glacier', region_name=REGION)

        try:
            response = glacier.initiate_job(
                accountId='-',
                jobParameters={
                    'Description': f'{s3_key_result_file}',
                    'ArchiveId': results_file_archive_id,
                    'Type': 'archive-retrieval',
                    'Tier': 'Expedited',
                    'SNSTopic': AWS_SNS_THAW_TOPIC,
                    },
                    vaultName=GLACIER_VAULT_NAME,
                    )
            retrieval_job_id = response['jobId']
            print(f"Expedited retrieval job initiated successfully. Job ID: {retrieval_job_id}")
        except Exception as e:
            print(f"Error retrieving archive with 'Expedited' retrieval. Message: {e}")

            # Then try standard version
            try:
                response = glacier.initiate_job(
                    accountId='-',
                    jobParameters={
                        'Description': 'Retrieve archive job',
                        'ArchiveId': results_file_archive_id,
                        'Type': 'archive-retrieval',
                        'Tier': 'Standard',
                        'SNSTopic': AWS_SNS_THAW_TOPIC,
                        },
                        vaultName=GLACIER_VAULT_NAME,
                        )
                retrieval_job_id = response['jobId']
                print(f"Standard retrieval job initiated successfully. Job ID: {retrieval_job_id}")
            except Exception as e:
                print(f"Error retrieving archive with 'Standard' retrieval. Message: {e}")
        
        # Persist retrieve job id just in case we need in the future
        update_expression = 'SET retrieval_job_id = :retrieve_id_value'
        expression_attribute_values = {':retrieve_id_value': retrieval_job_id}
        key = {"job_id": job_id}

        try:
          dynamo = boto3.resource('dynamodb')
          table = dynamo.Table(ANNOTATIONS_TABLE)
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
        
        # Delete message
        message.delete()
        print("Deleted message.")
                
### EOF