# Get auto generated secrets, add useful secret stuff

# TODO add aws-secretsmanager-caching as dependency

import botocore

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

__all__ = ['get_secret', 'get_secret_binary']

client = botocore.session.get_session().create_client('secretsmanager')
cache_config = SecretCacheConfig()
cache = SecretCache(config=cache_config, client=client)


def get_secret(secret_id, **kwargs):
    return cache.get_secret_string(secret_id=secret_id, **kwargs)


def get_secret_binary(secret_id, **kwargs):
    # TODO base64 decode
    return cache.get_secret_binary(secret_id=secret_id, **kwargs)
