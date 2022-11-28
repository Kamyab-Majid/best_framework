import pytest
import json
from pynutrien.aws.secrets import get_secret, get_secret_binary
from botocore.exceptions import ClientError

@pytest.fixture
def secret_name():
    return "insights/framework/test"


@pytest.fixture
def incorrect_secret_name():
    return "insightss/framework/test"

def test_get_secret_key(secret_name):
    string = get_secret(secret_name)
    secret_dict = json.loads(string)
    assert secret_dict["aws_test_access_id"] == '123456dwdqewrfe2'
    assert secret_dict["aws_test_secret_access_key"] == 'ewf6e4wf4ew65f45e6wf456'

def test_raise_exception_message_for_secret_key(incorrect_secret_name):
    with pytest.raises(ClientError):
        get_secret(incorrect_secret_name)

def test_get_secret_binary(secret_name):
    string = get_secret_binary(secret_name)
    assert string == None

if __name__ == '__main__':
    pytest.main()

