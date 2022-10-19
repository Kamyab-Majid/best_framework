# Glue interactive session for jupyter

Here we discuss the procedure to integrate a glue session in jupyter. Glue interactive sessions is for debugging on a small datasets,  please refer to the provided links in this document if the procedure is deprecated and update this document accordingly.

## debugging environment

Consider setting up a [virtual environment](https://docs.python.org/3/tutorial/venv.html) before using this feature.

## Python package Installation

Depending on your environment, you may need to run one of the following commands based on [glue_interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html):

``` bash
pip3 install --user --upgrade jupyter boto3 aws-glue-sessions
```

or

``` bash
pip install --user --upgrade jupyter boto3 aws-glue-sessions
```

## adding glue_spark extensions for jupyter

The following commands will add glue_spark/glue_pyspark extensions to jupyter based on [glue_interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html).

```bash
SITE_PACKAGES=$(pip3 show aws-glue-sessions | grep Location | awk '{print $2}')
jupyter kernelspec install $SITE_PACKAGES/aws_glue_interactive_sessions_kernel/glue_pyspark --user
jupyter kernelspec install $SITE_PACKAGES/aws_glue_interactive_sessions_kernel/glue_spark --user
```

## AWS CLI installation and configuration

### Installation

according to [getting started with AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
to install the AWS CLI use following commands:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws configure
```

Use your IAM role or user to configure the aws.

### configure aws using credentials

After installation, the command `aws sts get-caller-identity` should result in one of the followings:

```json
{
    "UserId": "THE_USER_ID",
    "Account": "ACCOUNT_NO",
    "Arn": "arn:aws:iam::123456789123:role/myIAMRole"
}
```

```json
{
    "UserId": "THE_USER_ID",
    "Account": "123456789123",
    "Arn": "arn:aws:iam::123456789123:user/MyIAMUser"
}

```

Which depends on if you set your aws for a user or based on a role. In first case which is when it is a role use:

```bash
aws iam attach-role-policy --role-name myIAMRole  --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
```

in the other case use:

```bash
aws iam attach-user-policy --user-name MyIAMUser --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
```

Which would give you enough access to use this feature.

## Prepare the AWS Glue service role for interactive sessions

You can specify the second principal, GlueServiceRole, either in the notebook itself by using the `%iam_role` magic or stored alongside the AWS CLI config. If you have a role that you typically use with AWS Glue jobs, this will be that role. If you don’t have a role you use for AWS Glue jobs, refer to [Setting up IAM permissions for AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html) to set one up.
To edit the aws config file use the following, you can change code with the editor of your choice (for example `nano`):

### using credentials file

```bash
code ~/.aws/credentials
```

add the following to your config file:

```txt
glue_role_arn=arn:aws:iam::Account:role/service-role/<ROLE>
region=<defualt_region>
```

in which `ARN` is the [ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html) of the role you made to be used for glue. You can find it using [IAM console](https://console.aws.amazon.com/iam/home) by searching the role name or using CLI:

```bash
aws iam get-role --role-name <role name>
```

### Using jupyter magic role

You can use multiple magic roles in the session in which a complete list of them are given in [Interactive session magic](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-magics.html) for lower cost we recommend the following magics:
Please have in mind that the below magics should be run **first before other cells are run** otherwise it will not affect the current session.
| Name                       | Type       | Description                                                                                                                                                      |
|----------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| %%configure                | Dictionary | Specify a JSON-formatted dictionary consisting of all configuration parameters for a session. Each parameter can be specified here or through individual magics. |
| %iam_role                  | String     | Specify an IAM role ARN to execute your session with. Default from ~/.aws/configure                                                                              |
| %number_of_workers         | int        | The number of workers of a defined worker_type that are allocated when a job runs. worker_type must be set too. The default number_of_workers is 5.              |
| %worker_type               | String     | Standard, G.1X, or G.2X. number_of_workers must be set too. The default worker_type is G.1X.                                                                     |
| %security_config           | String     | Define a Security Configuration to be used with this session.                                                                                                    |
| %connections               | List       | Specify a comma-separated list of connections to use in the session.                                                                                             |
| %additional_python_modules | List       | Comma separated list of additional Python modules to include in your cluster (can be from Pypi or S3).                                                           |
| %extra_py_files            | List       | Comma separated list of additional Python files from Amazon S3.                                                                                                  |
| %extra_jars                | List       | Comma-separated list of additional jars to include in the cluster.|
| %idle_timeout              | int         | The number of minutes of inactivity after which a session will timeout after a cell has been executed. The default idle timeout value is `2880` (minutes).|

The following options are available after each session is initiated.

| Name               | Type   | Description                                                                                                                                                         |
|--------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| %help              | n/a    | Return a list of descriptions and input types for all magic commands.                                                                                               |
| %profile           | String | Specify a profile in your AWS configuration to use as the credentials provider.                                                                                     |
| %region            | String | Specify the AWS Region; in which to initialize a session. Default from ~/.aws/configure.                                                                            |
| %idle_timeout      | Int    | The number of minutes of inactivity after which a session will timeout after a cell has been executed. The default idle timeout value is 2880 minutes (48 hours).   |
| %session_id        | String | Return the session ID for the running session. If a String is provided, this will be set as the session ID for the next running session.                            |
| %session_id_prefix | String | Define a string that will precede all session IDs in the format [session_id_prefix]-[session_id]. If a session ID is not provided, a random UUID will be generated. |
| %status            |        | Return the status of the current AWS Glue session including its duration, configuration and executing user / role.                                                  |
| %stop_session      |        | Stop the current session.                                                                                                                                           |
| %list_sessions     |        | Lists all currently running sessions by name and ID.                                                                                                                |
| %glue_version      | String | The version of Glue to be used by this session. Currently, the only valid options are 2.0 and 3.0. The default value is 2.0.                                        |
| %streaming         | String | Changes the session type to AWS Glue Streaming.                                                                                                                     |
| %etl               | String | Changes the session type to AWS Glue ETL.                                                                                                                           |

```python
%glue_version 3.0
%number_of_workers 2
%worker_type G.2X
%idle_timeout 15
```

each session is run after you run anything but the magic commands.
> **_NOTE:_**  Make sure to use `%idle_timeout` before each session is run and also to use `%stop_session` to avoid high costs. The cost is based on the number of workers per hour (0.44$ per hour) so also using least (2) `%number_of_workers 2` is recommended.
