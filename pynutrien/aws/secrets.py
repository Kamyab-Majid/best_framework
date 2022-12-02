from __future__ import annotations

import base64

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

from pynutrien.aws.boto import get_boto_session

__all__ = ["get_secret", "get_secret_binary", "get_secret_binary_decoded"]

# Setting up the client and session to get the passwords.
client = get_boto_session().client("secretsmanager")

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
        str: base64 encoded binary string for the given id.
    """
    return cache.get_secret_binary(secret_id=secret_id, **kwargs)


def get_secret_binary_decoded(secret_id: str, charset: str = "utf8", **kwargs) -> str:
    """get the binary string for the given secret id

    Args:
        secret_id (str): the id for the secret
        charset (str): the charset used for decoding the binary decoded data


    Returns:
        str: the secret string for the given id.
    """
    return base64.b64decode(get_secret_binary(secret_id, **kwargs)).decode(charset)
