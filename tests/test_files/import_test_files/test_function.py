import boto3

# Import the required packages
import aws_secretsmanager_caching
import redshift_connector
import s3fs
import pytz
import yaml
import tomli

# Create a new Lambda client
lambda_client = boto3.client('lambda')

# Define the name and runtime for your Lambda function
function_name = 'my_function'
runtime = 'python3.7'

# Define the function handler
def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'redshift_connector': str(redshift_connector.__version__),
        's3fs': str(s3fs.__version__),
        'yaml': str(yaml.__version__),
        'tomli': str(tomli.__version__),
    }
