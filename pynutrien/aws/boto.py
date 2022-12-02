from __future__ import annotations

import boto3
import botocore


def get_botocore_session() -> botocore.session.Session:
    return botocore.session.get_session()


def get_boto_session() -> boto3.session.Session:
    return boto3.session.Session(botocore_session=botocore.session.get_session())


def get_boto_client(service_name: str, **kwargs) -> botocore.client.BaseClient:
    return get_boto_session().client(service_name, **kwargs)
