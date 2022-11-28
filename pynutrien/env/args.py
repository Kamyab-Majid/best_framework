import argparse
import logging
import sys
from typing import Any

class ArgParser(argparse.ArgumentParser):
    """This class inherited from the argparse Python library acts as an argument parser
    giving user an interface to fully or partially parse arguments
    (a similar example of this class in AWS Glue is getResolvedOptions)
    """
    def __init__(self,*args, **kwargs) -> None:
        """ This is an inherited class from argparse library with additional methods to
        add and parse custom arguments.
        Also, a custom logger has also been set up when initializing this class
        """
        super().__init__(*args, **kwargs)
        self.logger = kwargs["logger"] if "logger" in kwargs else logging.getLogger(__name__)
        self.handler = kwargs["handler"] if "handler" in kwargs else logging.StreamHandler(
            sys.stdout)
        self.logging_level = kwargs["logging_level"] if "logging_level" in kwargs else 20
        self.format_string = '%(asctime)s::%(module)s::%(funcName)s::%(lineno)d::%(message)s'
        self.logger_format = logging.Formatter(
            kwargs["logger_format"] if "logger_format" in kwargs else self.format_string)

        self.handler.setFormatter(self.logger_format)
        self.logger.setLevel(self.logging_level)
        self.logger.addHandler(self.handler)

    def add_custom_arguments(self, args: dict) -> None:
        """This method takes a dictionary of custom arguments and add to the system to define
        how those arguments are taken on the command line to be parsed
        Args:
            args (dict): a list of arguments in dictionary format with key is the argument
            name and value is more details for the command-line argument
            in terms of action/default value/ required (if applicable)

        Raises:
            ValueError: when custom input arguments are not sufficient with information
            (not in dictionary format)
        """
        if not isinstance(args, dict):
            error_msg = \
                'Arguments passed in needs to be in dictionary format with key-value pair(s)'
            self.logger.exception(error_msg)
            raise ValueError(error_msg)

        for arg_name, arg_value in args.items():
            self.add_custom_argument(arg_name, arg_value)

    def add_custom_argument(self, arg_name: str, arg_value: Any) -> None:
        """This method is a sub-step in the loop of adding custom arguments.
        This takes both argument name and its value detail to define the command-line argument

        Args:
            arg_name (str): name of the custom argument to be added
            arg_value (Any): details (if any) of the custom argument.
            It could be a boolean (to store argument as true/false), a default value, a dictionary with key acts as a
            parameter of the add_argument() method (e.g. {required: True}),
            or it could be None (adding the argument without any specifications)
        """
        if arg_value:
            if isinstance(arg_value, bool):
                self.add_argument(arg_name, action=f'store_{str(arg_value).lower()}')
            elif isinstance(arg_value, dict):
                self.add_argument(arg_name, **arg_value)
            else:
                self.add_argument(arg_name, default=arg_value, type=type(arg_value))
        else:
            self.add_argument(arg_name)

    def parse(self, args: list[str] = None) -> dict:
        """This method convert custom argument strings to object and assign them as attributes of
        a result dictionary

        Args:
            args (list[str], optional): list of argument/option strings that wish to be parsed.
            Defaults to None (value is taken from sys.argv).

        Raises:
            ValueError: when there's no arguments passed before parsing

        Returns:
            dict: dictionary of results with argument name being the key and
            its parsed value being the value
        """
        parsed_dict = None
        existing_args = sys.argv[1:]
        if existing_args is None or len(existing_args) == 0:
            container = self._actions
            if container is None or len(container) < 2:
                error_msg = "There's no arguments passed in for parsing"
                self.logger.exception(error_msg)
                raise ValueError(error_msg)
        parsed, unknown = self.parse_known_args(args=args)
        parsed_dict = vars(parsed)
        return parsed_dict

    def parse_glue_args(self, glue_args: list[str] = []) -> dict:
        """This method is used specifically for parsing glue arguments

        Args:
            glue_args (list[str], optional): list of glue arguments to be parsed. Defaults to [].

        Returns:
            dict: dictionary of both parsed glue arguments and custom arguments
        """
        from awsglue.utils import getResolvedOptions

        glue_args = getResolvedOptions(sys.argv, glue_args)
        args = self.parse(sys.argv)
        return {**glue_args, **args}
