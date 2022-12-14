
import json
import subprocess
import yaml
import boto3
import uuid
import time
iam = boto3.client('iam')
role_name = 'lambda-role'
policy_name = 'my_lambda_policy'
function_name = 'test_function'
region = 'ca-central-1'
zip_file_name = 'my_layer.zip'
layer_zip_path = 'dist/lambda_layer.zip'
layer_name = 'my_layer'

sts = boto3.client('sts')

# Get the caller identity
identity = sts.get_caller_identity()

# Print the account ID
account_id = (identity['Account'])
command = f"aws iam create-role --role-name {role_name} --assume-role-policy-document '{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\"Principal\":{{\"Service\":\"lambda.amazonaws.com\"}},\"Action\":\"sts:AssumeRole\"}}]}}'"

# Run the command
output = subprocess.run(command, shell=True, capture_output=True)
get_arn_command = f'aws iam get-role --role-name {role_name}'
output = subprocess.run(get_arn_command, shell=True, capture_output=True)
role_dict = json.loads(output.stdout.decode())
role_arn = role_dict['Role']['Arn']


my_managed_policy = {
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
    response = iam.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(my_managed_policy)
    )
except:
    print('policy already exists')

# attach_command = "aws iam attach-role-policy --role-name lambda-role --policy-arn arn:aws:iam000ws:policy/AWSLambdaExecute"
policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'
attach_command = f'aws iam attach-role-policy --role-name {role_name} --policy-arn {policy_arn}'
output = subprocess.run(attach_command, shell=True, capture_output=True)
lambda_client = boto3.client('lambda')

layer_response = lambda_client.publish_layer_version(
    LayerName=layer_name,
    Content={
        'ZipFile': open(layer_zip_path, 'rb').read()
    },
    CompatibleRuntimes=['python3.7']
)
layer_arn = layer_response['LayerArn']

with open('tests/test_files/import_test_files/lambda_zip.zip', 'rb') as f:
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
print(f'Successfully created Lambda layer from "{zip_file_name}"')
print(f'Layer ARN: {layer_arn}')
print('hi')
delay = 1



# Check the current state of the function
function_details = lambda_client.get_function(FunctionName=function_name)
current_state = function_details['Configuration']['State']
print(f'Function {function_name} is in state {current_state}')

while True:
  try:
    function_details = lambda_client.get_function(FunctionName=function_name)
    current_state = function_details['Configuration']['State']
    if current_state=='Active':
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
print(response_value)
response = lambda_client.delete_layer_version(
    LayerName=layer_name, VersionNumber=layer_response['Version']
)
response = lambda_client.delete_function(
    FunctionName=function_name,
)

detach_command = f"aws iam detach-role-policy --role-name {role_name} --policy-arn {policy_arn}"

output = subprocess.run(detach_command, shell=True, capture_output=True)
response = iam.delete_policy(
    PolicyArn=policy_arn
)
response = iam.delete_role(
    RoleName=role_name,
)
