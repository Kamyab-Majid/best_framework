
import uuid
import json
import subprocess
import boto3
import time
from pathlib import Path


def test_lambda_imports():
    current_path = Path.cwd()
    abs_current_path = current_path.resolve()

    layer_zip_path = abs_current_path.joinpath/'tests'/'test_files'/'import_test_files'/'lambda_layer.zip'
    lambda_code_path = abs_current_path.joinpath/'tests'/'test_files'/'import_test_files'/'lambda_function.zip'
    iam = boto3.client('iam')
    role_name = 'lambda-role'
    policy_name = 'my_lambda_policy'
    function_name = 'test_function'
    region = 'ca-central-1'
    zip_file_name = 'my_layer.zip'
    layer_name = 'my_layer'
    # Get the account_id
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    account_id = (identity['Account'])
    make_rolecommand = f"aws iam create-role --role-name {role_name} --assume-role-policy-document '{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\"Principal\":{{\"Service\":\"lambda.amazonaws.com\"}},\"Action\":\"sts:AssumeRole\"}}]}}'"
    # Run the command
    role_output = subprocess.run(make_rolecommand, shell=True, capture_output=True)
    get_arn_command = f'aws iam get-role --role-name {role_name}'
    get_arn_output = subprocess.run(get_arn_command, shell=True, capture_output=True)
    role_dict = json.loads(get_arn_output.stdout.decode())
    role_arn = role_dict['Role']['Arn']

    lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "logs:CreateLogGroup",
                "Resource": f"arn:aws:lambda:{region}:{account_id}:function:{function_name}:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource":
                    f"arn:aws:lambda:{region}:{account_id}:function:{function_name}:*"
            }
        ]
    }
    try:
        policy_response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(lambda_policy)
        )
    except:
        print('Policy already exists')

    # attach_command
    policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'
    attach_command = f'aws iam attach-role-policy --role-name {role_name} --policy-arn {policy_arn}'
    attach_output = subprocess.run(attach_command, shell=True, capture_output=True)
    lambda_client = boto3.client('lambda')

    layer_response = lambda_client.publish_layer_version(
        LayerName=layer_name,
        Content={
            'ZipFile': open(layer_zip_path, 'rb').read()
        },
        CompatibleRuntimes=['python3.7']
    )
    layer_arn = layer_response['LayerArn']
    time.sleep(5)
    with open(lambda_code_path, 'rb') as f:
        function_response = lambda_client.create_function(
            FunctionName=function_name,
            Role=role_arn,
            Runtime='python3.7',
            Handler='test_function.lambda_handler',
            Layers=[layer_arn+f":{layer_response['Version']}"],
            Code={
                'ZipFile': f.read()
            }
        )
    delay = 1
    # Check the current state of the function
    function_details = lambda_client.get_function(FunctionName=function_name)
    current_state = function_details['Configuration']['State']
    print(f'Function {function_name} is in state {current_state}')

    while True:
        try:
            function_details = lambda_client.get_function(FunctionName=function_name)
            current_state = function_details['Configuration']['State']
            if current_state == 'Active':
                break
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f'Function {function_name} is not ready yet')
            time.sleep(delay)
    invoke_response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
    )
    response_stream = invoke_response.get('Payload')
    response_value = response_stream.read()
    delete_layer_response = lambda_client.delete_layer_version(
        LayerName=layer_name, VersionNumber=layer_response['Version']
    )
    delete_function_response = lambda_client.delete_function(
        FunctionName=function_name,
    )

    detach_command = f"aws iam detach-role-policy --role-name {role_name} --policy-arn {policy_arn}"

    detach_output = subprocess.run(detach_command, shell=True, capture_output=True)
    detach_response = iam.delete_policy(
        PolicyArn=policy_arn
    )
    delete_role_response = iam.delete_role(
        RoleName=role_name,
    )


def test_glue_imports():
    current_path = Path.cwd()
    abs_current_path = current_path.resolve()

    upper_path = abs_current_path.parent
    upper_upper_path = upper_path.parent
    layer_zip_path = abs_current_path.joinpath('dist', 'glue_layer.zip')
    glue_code_path = abs_current_path.joinpath('tests', 'test_files', 'import_test_files', 'glue_job.zip')
    iam = boto3.client('iam')
    role_name = 'glue-role'
    policy_name = 'glue_policy'
    function_name = 'test_glue_job'
    region = 'ca-central-1'
    zip_file_name = 'my_layer.zip'
    layer_name = 'my_layer'
    # Get the account_id
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()

    pass

import uuid
role_name = 'glue-role'
policy_name = 'glue_policy'
function_name = 'test_glue_job'
region = 'ca-central-1'
zip_file_name = 'my_layer.zip'
layer_name = 'my_layer'
bucket_name = 'glue-test-bucket'+uuid.uuid4().hex
region='ca-central-1'
# Create a boto3 client for the Glue service
iam=boto3.client('iam')

# Define the role's name and trust policy
role_name='MyGlueRole'
trust_policy={
    'Version': '2012-10-17',
    'Statement': [
        {
            'Effect': 'Allow',
            'Principal': {
                'Service': 'glue.amazonaws.com'
            },
            'Action': 'sts:AssumeRole'
        }
    ]
}

# Create the Glue role
role_response = iam.create_role(
    RoleName = role_name,
    AssumeRolePolicyDocument = json.dumps(trust_policy)
)
s3=boto3.client('s3')

# Create a new bucket
bucket_response=s3.create_bucket(
    Bucket = bucket_name,
    CreateBucketConfiguration = {
        'LocationConstraint': region
    }
)
# Print the ARN of the newly created role
print(role_response['Role']['Arn'])
# Define the policy document
policy_document={
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueJobPermissions",
            "Effect": "Allow",
            "Action": [
                "glue:GetJob",
                "glue:BatchStopJobRun",
                "glue:StartJobRun"
            ],
            "Resource": "*"
        },
        {
            "Sid": "AllowBucketReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }
    ]
}
sts = boto3.client('sts')
identity = sts.get_caller_identity()
account_id = (identity['Account'])
try:
    policy_response = iam.create_policy(
        PolicyName = policy_name,
        PolicyDocument = json.dumps(policy_document)
    )
except:
    print('Policy already exists')
policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'
# Attach the policy to the role

iam.attach_role_policy(
    RoleName=role_name,
    PolicyArn=policy_arn
)


# Create an S3 client

current_path = Path.cwd()
abs_current_path = current_path.resolve()
layer_zip_path = abs_current_path/'tests'/'test_files'/'import_test_files'/'glue_zip.zip'
glue_code_path = abs_current_path/'tests'/'test_files'/'import_test_files'/'glue_import_test.py'
s3.upload_file(str(layer_zip_path), bucket_name, 'glue_zip.zip')
s3.upload_file(str(glue_code_path), bucket_name, 'glue_import_test.py')
glue_config = {
    "Command": {
        "Name": "glueetl",
        "PythonVersion": "3",
        "ScriptLocation": f"s3://{bucket_name}/glue_import_test.py"
    },
    "DefaultArguments": {
        "--TempDir": f"s3://aws-glue-assets-{account_id}-{region}/temporary/",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-glue-datacatalog": "true",
        "--enable-job-insights": "true",
        "--enable-metrics": "true",
        "--enable-spark-ui": "false",
        "--extra-py-files": f"s3://{bucket_name}/glue_zip.zip",
        "--job-bookmark-option": "job-bookmark-enable",
        "--job-language": "python",
        "--spark-event-logs-path": f"s3://aws-glue-assets-{account_id}-{region}/sparkHistoryLogs/"
    },
    "Description": "",
    "ExecutionProperty": {
        "MaxConcurrentRuns": 1
    },
    "GlueVersion": "3.0",
    "MaxRetries": 0,
    "Name": "glue_import_test",
    "NumberOfWorkers": 2,
    "Role": f"arn:aws:iam::{account_id}:role/service-role/{role_name}",
    "Timeout": 5,
    "WorkerType": "G.1X"
}
glue_client = boto3.client('glue')
glue_client.create_job(**glue_config)
glue_client.start_job_run(JobName=glue_config['Name'])
glue_client.get_job_runs(JobName=glue_config['Name'])
glue_client.delete_job(JobName=glue_config['Name'])
