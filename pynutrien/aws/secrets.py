# Get auto generated secrets, add useful secret stuff

# TODO add aws-secretsmanager-caching as dependency

import botocore
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

__all__ = ["get_secret", "get_secret_binary"]

# Setting up the client and session to get the passwords.
client = botocore.session.get_session().create_client(service_name='secretsmanager')

# Initiating the cache config to store the retrieved passwords.
cache_config = SecretCacheConfig()
cache = SecretCache(config=cache_config, client=client)



def get_secret(secret_id: str, **kwargs) -> str:
    """get the string for the given secret id

    Args:
        secret_id (str): the id for the secret

    Returns:
        str: _description_
    """
    return cache.get_secret_string(secret_id=secret_id, **kwargs)



def get_secret_binary(secret_id: str, **kwargs) -> str:
    """get the binary string for the given secret id

    Args:
        secret_id (str): the id for the secret


    Returns:
        str: binary string for the given id.
    """
    # TODO base64 decode
    return cache.get_secret_binary(secret_id=secret_id, **kwargs)
