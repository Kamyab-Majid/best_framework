from __future__ import annotations

import sys

import pytest

from pynutrien.core import ArgParser


@pytest.fixture
def arg_parser():
    return ArgParser()


# arguments is fully parsed then return a dictionary of parsed argument
def test_when_args_are_parsed_then_return_dict(arg_parser):
    args = {
        "--debug_run": True,
        "--input_connector": None,
        "--debug_level": "INFO",
        "--value": {"required": True},
    }
    arg_parser.add_custom_arguments(args)
    parsed_dict = arg_parser.parse(["--debug_run", "--input_connector", "a", "--debug_level", "b", "--value", "c"])
    assert isinstance(parsed_dict, dict)
    assert parsed_dict["input_connector"] == "a"


def test_when_args_passed_in_is_not_dict_should_raise_value_error(arg_parser):
    args = 2
    with pytest.raises(ValueError):
        arg_parser.add_custom_arguments(args)


def test_when_args_should_be_parsed_as_bool_only(arg_parser):
    args = {"--debug_run": True}
    arg_parser.add_custom_arguments(args)
    parsed_dict = arg_parser.parse(["--debug_run", "a"])
    assert parsed_dict["debug_run"]
    assert parsed_dict["debug_run"] != "a"


def test_when_args_are_parsed_as_none(arg_parser):
    args = {"--debug_run": None}
    arg_parser.add_custom_arguments(args)
    parsed_dict = arg_parser.parse("--debug_run")
    assert parsed_dict["debug_run"] is None


def test_when_args_are_set_to_a_default_value(arg_parser):
    args = {"--debug_run": "INFO"}
    arg_parser.add_custom_arguments(args)
    parsed_dict = arg_parser.parse("--debug_run")
    assert parsed_dict["debug_run"] == "INFO"


def test_when_args_are_set_as_required_but_not_included_should_fail(arg_parser):
    args = {"--debug_run": {"required": True}}
    arg_parser.add_custom_arguments(args)
    with pytest.raises(SystemExit):
        arg_parser.parse()

    with pytest.raises(SystemExit):
        arg_parser.parse(["--value"])


# not exist in arguments but exist in option (partial parse - only parse the existing arguments)
def test_when_args_are_partially_parsed_should_work(arg_parser):
    args = {"--debug_run": "INFO"}
    arg_parser.add_custom_arguments(args)
    parsed_dict = arg_parser.parse(["--debug_run", "DEBUG", "--value"])
    assert parsed_dict["debug_run"] == "DEBUG"
    assert "value" not in parsed_dict


# no arguments are added/existed before parsing should raise a ValueError
def test_when_arguments_is_empty_before_parsing(arg_parser):
    args = {}
    existing_args = sys.argv[1:]

    arg_parser.add_custom_arguments(args)
    if existing_args is None or len(existing_args) == 0:
        with pytest.raises(ValueError):
            arg_parser.parse()

        with pytest.raises(ValueError):
            arg_parser.parse(["--debug-run", "--config2"])
