# import boto3
from __future__ import annotations

from pynutrien.aws.boto import get_boto_session

emr = get_boto_session().client("emr")

__all__ = ["run_job_flow", "start_notebook_execution", "stop_notebook_execution"]


def run_job_flow(**kwargs):
    emr.run_job_flow(**kwargs)


def start_notebook_execution(**kwargs):
    emr.start_notebook_execution(**kwargs)


def stop_notebook_execution(**kwargs):
    emr.stop_notebook_execution(**kwargs)
