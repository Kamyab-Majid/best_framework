import pytest
from pynutrien.env.config import ConfigReader

@pytest.fixture
def cf():
    return ConfigReader()

@pytest.fixture(
    params=[
        (
            "tests/config_parser_test_data/config.json",
            "json",
            {'size': 'small', 'color': 'red', 'country': 'USA'}
        ),
        (
            "tests/config_parser_test_data/config.ini",
            "ini",
            {'size': 'small', 'color': 'red', 'country': 'USA'}
        ),
        (
            "tests/config_parser_test_data/fruits.yaml",
            "yaml",
            {'size': 'small', 'color': 'red', 'country': 'USA'}
        ),
        (
            "tests/config_parser_test_data/config.toml",
            "toml",
            {'player_x': {'color': 'blue'}, 'player_o': {'color': 'green'}}
        )
    ],
    ids=[
        "json_config", "ini_config", "yaml_config", "toml_config"
    ],
    scope="session")

def valid_config(request):
    return request.param

@pytest.fixture(
    params=[
        ("tests/config_parser_test_data/empty_config.json", "json"),
        ("tests/config_parser_test_data/empty_config.ini", "ini"),
        ("tests/config_parser_test_data/empty_fruits.yaml", "yaml"),
        ("tests/config_parser_test_data/empty_config.toml", "toml")],
    ids=["json_file", "ini_file", "yaml_file", "toml_file"],
    scope="session")
    
def empty_config(request):
    return request.param

@pytest.fixture
def invalid_config_file_extension():
    return ("tests/config_parser_test_data/config.txt", "txt")

@pytest.fixture
def invalid_json_config():
    return ("tests/config_parser_test_data/invalid_config.json", "json")

@pytest.fixture
def invalid_ini_config():
    return ("tests/config_parser_test_data/invalid_config.ini", "ini")

@pytest.fixture
def invalid_yaml_config():
    return ("tests/config_parser_test_data/invalid_fruits.yaml", "yaml")

@pytest.fixture
def invalid_toml_config():
    return ("tests/config_parser_test_data/invalid_config.toml", "toml")

def get_file_content(path: str):
    fileObject = open(path, "r")
    return fileObject.read()

def test_main_parser_with_valid_config(cf, valid_config):
    file_content = get_file_content(valid_config[0])
    parsed_config = cf.parse(file_content, valid_config[1])
    assert isinstance(parsed_config, dict)
    assert bool(parsed_config)

def test_main_parser_with_empty_config(cf, empty_config):
    file_content = get_file_content(empty_config[0])
    parsed_config = cf.parse(file_content, empty_config[1])
    assert isinstance(parsed_config, dict)
    assert not bool(parsed_config)

def test_main_parser_with_invalid_file_type(cf, invalid_config_file_extension):
    file_content = get_file_content(invalid_config_file_extension[0])
    with pytest.raises(NotImplementedError):
        cf.parse(file_content, invalid_config_file_extension[1])

def test_particular_file_parser_with_valid_config(cf, valid_config):
    file_content = get_file_content(valid_config[0])
    expected_result = valid_config[2]
    if valid_config[1] == "json":
        parsed_config = cf.parse_json(file_content)
        config_to_validate = parsed_config['fruit']['apple']
    elif valid_config[1] == "ini":
        parsed_config = cf.parse_ini(file_content)
        config_to_validate = parsed_config['apple']
    elif valid_config[1] == "yaml":
        parsed_config = cf.parse_yaml(file_content)
        config_to_validate = parsed_config['fruits']['apple']
    elif valid_config[1] == "toml":
        parsed_config = cf.parse_toml(file_content)
        config_to_validate = parsed_config['user']
    assert isinstance(parsed_config, dict)
    assert config_to_validate == expected_result

def test_json_parser_with_invalid_config(cf, invalid_json_config):
    file_content = get_file_content(invalid_json_config[0])
    with pytest.raises(RuntimeError):
        cf.parse_json(file_content)

def test_ini_parser_with_invalid_config(cf, invalid_ini_config):
    file_content = get_file_content(invalid_ini_config[0])
    with pytest.raises(RuntimeError):
        cf.parse_ini(file_content)

def test_yaml_parser_with_invalid_config(cf, invalid_yaml_config):
    file_content = get_file_content(invalid_yaml_config[0])
    with pytest.raises(RuntimeError):
        cf.parse_yaml(file_content)

def test_toml_parser_with_invalid_config(cf, invalid_toml_config):
    file_content = get_file_content(invalid_toml_config[0])
    with pytest.raises(RuntimeError):
        cf.parse_toml(file_content)
