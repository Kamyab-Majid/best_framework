import json
import configparser
import tomli
import yaml

class ConfigReader:
    """
    A class used for reading a config file (JSON, YAML, TOML, INI)
    """

    def __init__(self) -> None:
        """
        init function used to initialize the object of the class
        """

    def parse(self, file_content: str, file_type: str) -> dict:
        """
        This method acts as a main parser to parse a string content of a given file type
        into a python dictionary

        Args:
            file_content (str): The content string of the json file to be parsed.
            file_type (str): The type of the config file to be read/parse
            ('json', 'yaml', 'toml', 'ini')

        Returns:
            dict: a python dictionary of the config content.
        """
        try:
            return eval(f"self.parse_{file_type.strip().lower()}(file_content)")
        except AttributeError as attr_exc:
            raise NotImplementedError(
                f"Reader method has not been implemented for this file type '{file_type}'"
                ) from attr_exc

    def parse_json(self, file_content: str) -> dict:
        """
        This method parses the string content of a JSON file into a python dictionary

        Args:
            file_content (str): The content string of the json file to be parsed.

        Returns:
            dict: a python dictionary of the json content.
        """
        try:
            return json.loads(file_content)
        except json.decoder.JSONDecodeError as exc:
            raise RuntimeError("This file content is not JSON format.") from exc


    def parse_ini(self, file_content: str) -> dict:
        """
        This method parses the content of the .ini file from a string value
        to a python dictionary

        Args:
            file_content (str): The content string of the ini file to be parsed.

        Returns:
            dict: a python dictionary of the ini string content
            (IMPORTANT: The specific key values need to be specified when you call the method
            in square brackets. For example VARIABLENAME['Key']['subkey])
        """
        try:
            config = configparser.ConfigParser()
            config.read_string(file_content)
            output = {s:dict(config.items(s)) for s in config.sections()}
            return output
        except configparser.ParsingError as exc:
            raise RuntimeError("This file content is not INI format.") from exc

    def parse_yaml(self, file_content: str) -> dict:
        """
        This method uses the yaml package to interpret the string content
        and convert it into a python dictionary.

        Args:
            file_content (str): The content string of the yaml file to be parsed.

        Returns:
            dict: a python dictionary of the yaml string content.
        """
        if len(file_content) == 0:
            return {}
        try:
            output = yaml.load(file_content, Loader=yaml.Loader)
            return output
        except yaml.scanner.ScannerError as exc:
            raise RuntimeError("This file content is not YAML format.") from exc

    def parse_toml(self, file_content: str) -> dict:
        """
        This method uses the tomli library to read the string content from the toml file
        and convert it into a python dictionary.

        Args:
            file_content (str): The content string of the toml file to be parsed.

        Returns:
            dict: a python dictionary of the toml string content.
        """
        try:
            output = tomli.loads(file_content)
            return output
        except tomli.TOMLDecodeError as exc:
            raise RuntimeError("This file content is not TOML format.") from exc


if __name__ == "__main__":
    json_text = """
    {
    "fruit": {
        "apple": {
            "size": "small",
            "color": "red",
            "country": "USA"
        },
        "banana":{
            "size": "medium",
            "color": "yellow",
            "country": "Fiji"
        },
        "orange":{
            "size": "large",
            "color": "orange",
            "country": "Egypt"
        }
    }
}
    """
    yaml_text = """
        name: "Vishvajit"
        age: 23
        address: Noida
        Occupation: Software Developer
        Skills:
        - Python
        - Django
        - Flask
        - FastAPI
        - DRF ( Django Rest Framework )
    """
    toml_text = """
        [user]
        player_x.color = "blue"
        player_o.color = "green"

        [constant]
        board_size = 3

        [server]
        url = "https://tictactoe.example.com"
    """
    ini_text = """
        [apple]
        size = small
        color = red
        country = USA
    """
    sample_dict = {
        "Vegetable": {
            "carrot": {"size": "cylindrical", "color": "orange", "country": "America"}
        }
    }
    a = ConfigReader(json_text)
    b = ConfigReader(ini_text)
    c = ConfigReader(yaml_text)
    d = ConfigReader(toml_text)
    print(a.merge_dictionaries(sample_dict))
    # print(c.parse_yaml())
    # print(d.parse_toml())
    # print(b.parse_ini())
