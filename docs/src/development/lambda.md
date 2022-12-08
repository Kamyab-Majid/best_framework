==================
Lambda Development
==================

### Layers and dependencies

Each lambda is assumed to be using [`insights-framework`](https://github.com/Nutrien/insights-framework) as a dependency. It will be provided by a layer, with it's dependencies.
Some other dependencies can be added as a layer, e.g.

- [AWSWrangler layer info](https://serverlessrepo.aws.amazon.com/applications/us-east-1/336392948345/aws-data-wrangler-layer-py3-7)
- [AWS-lambda-powertools](https://serverlessrepo.aws.amazon.com/applications/eu-west-1/057560766410/aws-lambda-powertools-python-layer-extras)

## Development

There are several options for developing a lambda function.

- Use the Dockerfile to build and test/simulate a lambda function
- They can be developed, tested, and debugged in an IDE as a normal python function
- Setup a local jupyter notebook (or use the glue image to run jupyter)
- Develop/test in the AWS console and use the event simulator


- [AWS Lambda Developer guide](https://github.com/awsdocs/aws-lambda-developer-guide)
- [AWS Sample Apps](https://github.com/awsdocs/aws-lambda-developer-guide/tree/main/sample-apps/blank-python)

### Requirements

Define a function of two arguments (`event`, `context`). Define the entry point as `source_file_name.callable_object`.

- `event` a dictionary with any sub-structure
- [context](https://docs.aws.amazon.com/lambda/latest/dg/python-context.html)


```python

def lambda_handler(event, context):
    return {}

```


### Creation of packaged layers

[Python packages](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)

If you have a python virtual environment, you can zip the `site-packages` directory to include in the deployment zip as a layer. The same can be achieved by adding a `requirements.txt` file in this directory.
Use the docker image to create the virtual environment so that the already included python packages are excluded from the dependency tree.

### Invoking other services
### Requirements

Define a function of two arguments (`event`, `context`). Define the entry point as `source_file_name.callable_object`.

- `event` a dictionary with any sub-structure
- [context](https://docs.aws.amazon.com/lambda/latest/dg/python-context.html)


```python

def lambda_handler(event, context):
    return {}

```


### Creation of packaged layers

[Python packages](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)

If you have a python virtual environment, you can zip the `site-packages` directory to include in the deployment zip as
a layer. The same can be achieved by adding a `requirements.txt` file in this directory. Use the docker image to create
the virtual environment so that the already included python packages are excluded from the dependency tree.

### Invoking other services

Some environment services will be named consistently in all environments. Otherwise, they will follow the naming
convention:

`insights_${AWS_REGION}_${AWS_ENV_NAME}_${PROJECT_NAME}_${NAME}`

or for generics with no associated project:

`insights_${AWS_REGION}_${AWS_ENV_NAME}_${NAME}`

These should be set as environment variables for every lambda, then you can use the insights `Environment` to obtain the
environment specific name.

```python
from pynutrien.core import Environment
# Get real environment name
env_name = Environment().get_name("sample_lambda_function")
# Get real environment name for generic
env_name = Environment().get_generic_name("sample_lambda_function")
# Get another region name
env_name = Environment(region='use1').get_name("sample_lambda_function")
```
## Deployment

The files in same directory all belong to the lambda function. They will be zipped and deployed. It's possible to deploy
multiple files or a helper file along with the file with defining the `lambda_handler`. Shared functionality should be added to the
shared library and accessed that way instead.

