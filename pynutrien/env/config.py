import logging
import json
import tomli        
import configparser 
import yaml
from pathlib import Path

filepath = Path(r"C:\Users\Murtaza.Badshah\Documents\GitHub\insights-framework\pynutrien\env")

logging.basicConfig(filename=filepath/'logfile.txt', level=logging.INFO,
                format='%(asctime)s:%(levelname)s:%(message)s')

class ConfigReader:
    """
    A class used for reading a config file.
    ...

    Attributes
    ----------
    filename: str
        this is the name of the file along with the extension

    Methods
    -------
    parseConfig(self)
        reads the file from the specified folder.
        based on the file extension converts the
        file into a python dictionary
    readFileExt(self)
        takes the entered file name splits the
        text and returns just the extension name.
    """

    def __init__(self, file_string:str) -> None:
        """
        init function used to initialize the object of the class
        ...
        Args:
            filename (string): name of the file the user inputs.
        """
        self.file_string = file_string
        logging.info('Created file string: {}'.format(self.file_string))


    def parse_json(self) -> dict:
        """This method parses the string value (json string)
        into a python dictionary

        Returns:
            dict: a python dictionary of the json string.
        """
        try:
            output = json.loads(self.file_string)
            logging.info('Success! The json string was converted into {}'.format(type(output)))
            return output
        except Exception as e:
            logging.error("Incorrect Format of string variable", exc_info=True)
            


    def parse_ini(self) -> dict:
        """This method takes the .ini files as a string
        value and returns a python dictionary by using
        the config parser package.

        Returns:
            dict: a python dictionary of the ini string
            (IMPORTANT: The specific key values need
            to be specified when you call the method
            in square brackets for example 
            VARIABLENAME['Key']['subkey])
        """
        try:
            config = configparser.ConfigParser()
            config.read_string(self.file_string)
            # Code referenced from https://stackoverflow.com/questions/1773793/convert-configparser-items-to-dictionary
            output = {s:dict(config.items(s)) for s in config.sections()} 
            logging.info('Success! The ini string was converted into {}'.format(type(output)))
            return output
        except Exception as e:
            logging.error("Incorrect Format of string variable", exc_info=True)
            
            

    def parse_yaml(self) -> dict:
        """Uses the yaml package to interpret the string
        and convert it into a python dictionary.

        Returns:
            dict: a python dictionary of the yaml string.
        """
        try:
            output = yaml.load(self.file_string, Loader=yaml.Loader)
            logging.info('Success! The yaml string was converted into {}'.format(type(output)))
            return output
        except Exception as e:
            logging.error("Incorrect Format of string variable", exc_info=True)


    def parse_toml(self) -> dict:
        """Uses the tomli library to read the toml string
        and convert it into a python dictionary.

        Returns:
            dict: a python dictionary of the toml string.
        """
        try:
            output = tomli.loads(self.file_string)
            logging.info('Success! The toml string was converted into {}'.format(type(output)))
            return output
        except Exception as e:
            logging.error("Incorrect Format of string variable", exc_info=True)


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
    sample_dict = {"Vegetable": {"carrot":{"size": "cylindrical", "color": "orange", "country": "America"}}}
    a = ConfigReader(json_text)
    b = ConfigReader(ini_text)
    c = ConfigReader(yaml_text)
    d = ConfigReader(toml_text)
    print(a.merge_dictionaries(sample_dict))
    #print(c.parse_yaml())
    #print(d.parse_toml())
    #print(b.parse_ini())