
import boto3

emr = boto3.client('emr')  # TODO region

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.start_notebook_execution

def run_job_flow(**kwargs):
    emr.run_job_flow(**kwargs)

def start_notebook_execution(**kwargs):
    emr.start_notebook_execution(**kwargs)

def stop_notebook_execution(**kwargs):
    emr.stop_notebook_execution(**kwargs)
