
import json
import subprocess
import boto3
import time
from pathlib import Path


def test_lambda_imports():
    current_path = Path.cwd()
    abs_current_path = current_path.resolve()

    upper_path = abs_current_path.parent
    upper_upper_path = upper_path.parent
    layer_zip_path = abs_current_path.joinpath('dist', 'lambda_layer.zip')
    lambda_code_path = abs_current_path.joinpath('tests','test_files', 'import_test_files', 'lambda_function.zip')
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
        policy_response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(my_managed_policy)
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
