# run.py
import pytest
import sys


def main():
    """This func should be called whenever a full test is required. Since some tests require sys.argv"""
    pytest.main([sys.argv[1]])


if __name__ == "__main__":
    main()
