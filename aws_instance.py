import boto3
from botocore.config import Config
import time

def create_instance(hw_str, region="us-east-1", cnet_id="josemaria",
                    ami="ami-0511f6efd2d842fde", instance="t2.nano",
                    wait_bool=True):
    """
    Create an EC2 instance with different parameters.

    Args:
        hw_str (`str`): homework text. For example: 'hw3'.
        region (`str`): region name of instance. By default it's set to
            'us-east-1'
        cnet_id (`str`): CNET ID of person who is creating the instance. By
            default it's set to 'josemaria'.
        ami (`str`): AMI (Amazon Machine Image) ID for instance. By default it's
            set to 'ami-0627971cd8c777781'.
        instance (`str`): instance type. By default it's set to 't2.nano'.
    
    Returns (`tup` of `ec2.Resource` and `ec2.Instance`): ec2 resource and
        instance. It also prints the instance ID and Public DNS. If there was an
        error creating the instance it returns a string with the error.
    """
    
    config = Config(region_name=region)
    ec2 = boto3.resource('ec2', config=config)
    
    try:
        instances = ec2.create_instances(
            MinCount=1,
            MaxCount=1,
            ImageId=ami,
            InstanceType=instance,
            IamInstanceProfile={'Name': 'instance_profile_' + cnet_id},
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': cnet_id + "-" + hw_str}]
                }],
                KeyName="josemaria-3",
                SecurityGroups=['mpcs-cc']
                )
    except Exception as e:
        return f"Error when creating the instance. error output: {e}"
    
    instance = instances[0]

    # Wait until instance is running to print ID and DNS
    if wait_bool:
        instance.wait_until_running()
        instance.reload()

    print("Instance ID: ", instance.id)
    print("Public DNS: ", instance.public_dns_name)

    return ec2, instance

def stop_instance(instance_id, region="us-east-1"):
    """
    Stop an EC2 instance.

    Args:
        instance_id (`str`): instance ID 
    """
    
    # Resources:
    # - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/stop_instances.html
    
    config = Config(region_name=region)

    ec2 = boto3.client('ec2', config=config)
    
    try:
        response = ec2.stop_instances(
            InstanceIds=[
                instance_id,
                ],
                )
    
    except Exception as e:
        return f"Error when trying to stop instance. error: {e}"
    
    return response
    
def create_s3_folder(bucket, folder, region="us-east-1"):
    """
    Create 'folder' inside an s3 bucket to group file objects.

    Args:
        bucket (`str`): bucket name. It can be either 'mpcs-cc-gas-inputs' or
            'mpcs-cc-gas-results'
        folder (`str`): folder name we want to create inside s3 bucket.
    
    Returns: None. Creates folder in bucket.
    """

    # Resources:
    # - https://github.com/boto/boto3/discussions/3286

    config = Config(region_name=region)
    client = boto3.client('s3', config=config)
    folder += "/"

    try:
        client.put_object(
            Bucket=bucket,
            Key=folder
        )
    except Exception as e:
        return f"Error when trying to create s3 folder '{folder}' in bucket '{bucket}'. error: {e}"
    
def get_aws_credentials():
    """
    Get AWS credentials.

    Returns (`tuple`): tuple with AWS access key and secret key.
    """
    # Resources:
    # - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.get_credentials

    credentials = boto3.Session().get_credentials()
    frozen_credentials = credentials.get_frozen_credentials()
    
    access_key = frozen_credentials.access_key
    secret_key = frozen_credentials.secret_key

    return access_key, secret_key

def create_dynamo_table(table_name):
    """
    Create a DynamoDB table object.

    Args:
        table_name (`str`): table name.
    
    Returns (`dynamodb.Table`): DynamoDB table object. 
    """
    try:
        dynamo = boto3.resource('dynamodb')
        table = dynamo.Table(table_name)
        return table

    except Exception as e:
        return {"code": 500,
                "error": f"Error trying to create dynamo table '{table_name}'.",
                "message": str(e)}
    
def get_table_item(table_name, primary_key, key_value):
    """
    Get item from DynamoDB table object by it's primary key.

    Args:
        table_name (`str`): table name. Set to 'josemaria_annotations' by
            default
        primary_key (`str`): primary key name to access table item
        key_value (`str`): primary key value
    
    Returns (`dict`): response with item's keys and values.
    """
    table = create_dynamo_table(table_name)
    item_key = {primary_key: key_value}
    response = table.get_item(Key=item_key)

    return response