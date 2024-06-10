# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

# Resources:
# - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
# - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.FilterExpression.html
# - https://stackoverflow.com/questions/35758924/how-do-we-query-on-a-secondary-index-of-dynamodb-using-boto3

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""

@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']

  user_id = session['primary_identity']


  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  _, user_id, job_file = s3_key.split("/")
  job_id, input_file_name = job_file.split("~")
  submit_time = int(time.time())

  # Get email
  profile = get_profile(identity_id=session.get('primary_identity'))
  email = profile.email
  user_role = profile.role

  # Persist job to database
  data = { "job_id": job_id,
          "user_id": user_id,
          "input_file_name": input_file_name,
          "s3_inputs_bucket": bucket_name,
          "s3_key_input_file": s3_key,
          "submit_time": submit_time,
          "job_status": "PENDING",
          "email": email,
          "user_role": user_role
          }
  
  try:
    dynamo = boto3.resource('dynamodb')
    table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
    table = dynamo.Table(table_name)
  except Exception as e:
    return {"code": 500,
            "error": f"Error trying to create dynamo table '{table_name}'.",
            "message": str(e)}
    
  try:
    response = table.put_item(Item = data)
  except Exception as e:
    return {"code": 500,
            "error": f"Error trying to create item in table '{table_name}'.",
            "message": str(e),
            "item": data}

  # Send message to request queue
  try:
    client = boto3.client('sns')
  except Exception as e:
    return {"code": 500,
            "error": "Error trying to create SNS client.",
            "message": str(e)}
    
  topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
  message = str(data)
    
  try:
    response = client.publish(
      TopicArn=topic_arn,
      Message=message
      )
    message_id = response["MessageId"]
  except Exception as e:
    return {"code": 500,
            "error": "Error trying to publish message to SNS client.",
            "message": str(e),
            "topic_arn": topic_arn,
            "sns_message": message}
    
  data = {"job_id": job_id, "file_name": input_file_name, "message_id": message_id}

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  # Get list of annotations to display
  user_id = session['primary_identity']
  table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']

  dynamodb = boto3.client('dynamodb')

  index_name = 'user_id_index'
  key_condition_expression = 'user_id = :value'
  user_id_str = str(user_id)
  expression_attribute_values = {':value': {'S': user_id_str}}

  # Perform the query
  response = dynamodb.query(
      TableName=table_name,
      IndexName=index_name,
      KeyConditionExpression=key_condition_expression,
      ExpressionAttributeValues=expression_attribute_values
  )

  job_list = response["Items"]

  # Remove the data type from the key values so they look prettier
  new_job_list = []
  for item in job_list:
      d = {}
      for key, value in item.items():
          new_value = list(value.values())[0]
          d[key] = new_value
      new_job_list.append(d)

  return render_template('annotations.html', annotations=new_job_list)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  pass


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  pass


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    
    # First we have to get the archive_id list we want to send to our restore SNS topic
    user_id = session['primary_identity']
    table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']

    try:
      dynamodb = boto3.resource('dynamodb')
      table = dynamodb.Table(table_name)
    except Exception as e:
      return {"code": 500,
            "error": "Error trying to create DynamoDB table instance.",
            "message": str(e)}

    index_name = 'user_id_index'
    user_id_str = str(user_id)

    # Perform the query
    try:
      response = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('user_id').eq(user_id_str),
        FilterExpression=Attr('job_status').eq('COMPLETED') & Attr('results_file_archive_id').exists()
        )
    except Exception as e:
      return {"code": 500,
            "error": f"Error trying to query table {table_name}.",
            "message": str(e)}

    job_list = response["Items"]

    for job in job_list:
      archive_id = job["results_file_archive_id"]
      job_id = job["job_id"]
      s3_key_result_file = job["s3_key_result_file"]
      message_dict = {"results_file_archive_id": archive_id,
                      "job_id": job_id, "s3_key_result_file": s3_key_result_file}
      message_str = str(message_dict)
      
      try:
        client = boto3.client('sns')
      except Exception as e:
        return {"code": 500,
                "error": "Error trying to create SNS client.",
                "message": str(e)}
      
      restore_sns_topic = app.config['AWS_SNS_JOB_RESTORE_TOPIC']

      try:
        response = client.publish(
          TopicArn=restore_sns_topic,
          Message=message_str
          )
        message_id = response["MessageId"]
      except Exception as e:
        print({"code": 500,
              "error": "Error trying to publish message to SNS client.",
              "message": str(e),
              "restore_sns_topic": restore_sns_topic,
              "sns_message": message_str})
      
      # Update new user_profile in DynamoTable
      user_role = "premium_user"
      update_expression = 'SET user_role = :new_role'
      expression_attribute_values = {':new_role': user_role}
      key = {"job_id": job_id}

      try:
        response = table.update_item(
          Key=key,
          UpdateExpression=update_expression,
          ExpressionAttributeValues=expression_attribute_values
          )
      except Exception as e:
        print(f"Error when trying to update Dynamo DB object. Message: {e}")
      
    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
