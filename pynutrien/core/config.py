from __future__ import annotations

import re

import s3fs

__all__ = ["ConfigParser", "ConfigReader"]


class ConfigParser:
    """
    A class used for reading a config file (JSON, YAML, TOML, INI)
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
            return getattr(self, f"parse_{file_type.strip().lower()}")(file_content)
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
        import json

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
        import configparser

        try:
            config = configparser.ConfigParser()
            config.read_string(file_content)
            output = {s: dict(config.items(s)) for s in config.sections()}
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
        import yaml

        if len(file_content) == 0:
            return {}
        try:
            output = yaml.load(file_content, Loader=yaml.Loader)
            return output
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as exc:
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
        import tomli

        try:
            output = tomli.loads(file_content)
            return output
        except tomli.TOMLDecodeError as exc:
            raise RuntimeError("This file content is not TOML format.") from exc


class ConfigReader(ConfigParser):
    s3 = s3fs.S3FileSystem(anon=False)

    def __init__(self, path: "str|None" = None):
        self.path = path
        self.ext = None
        if "." in self.path:
            ext = path.split(".")[-1].lower()
            if ext in ("json", "yaml", "toml", "ini"):
                self.ext = ext
        self._file_contents = None

    @property
    def file_contents(self):
        if self._file_contents is None:
            if re.match(r"s3[an]?://", self.path):
                with self.s3.open(self.path, "r") as fh:
                    self._file_contents = fh.read()
            else:
                with open(self.path, "r") as fh:
                    self._file_contents = fh.read()
        return self._file_contents

    def read(self, file_type: "str|None" = None) -> dict:
        file_type = file_type or self.ext
        if file_type is None:
            raise RuntimeError(f"Cannot identify file format from uri: '{self.path}'")
        return self.parse(self.file_contents, self.ext)

    def read_json(self) -> dict:
        return self.parse_json(self.file_contents)

    def read_ini(self) -> dict:
        return self.parse_ini(self.file_contents)

    def read_yaml(self) -> dict:
        return self.parse_yaml(self.file_contents)

    def read_toml(self) -> dict:
        return self.parse_toml(self.file_contents)
