# run.py
from __future__ import annotations

import sys

import pytest


def main():
    """This func should be called whenever a full test is required. Since some tests require sys.argv"""
    pytest.main([sys.argv[1]])


if __name__ == "__main__":
    main()
