#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [
    "Click>=7.0",
]

test_requirements = [
    "pytest>=3",
]

setup(
    author="Majid Kamyab",
    author_email="majid.kamyab@nutrien.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="A shared python library repository - will have sphinx/unittests etc. the sphinx and build need to be configured - and if you are using poetry, that will take a while to set all the parameters, get all the sphinx modules etc.",
    entry_points={
        "console_scripts": [
            "pynutrien=pynutrien.cli:main",
        ],
    },
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="pynutrien",
    name="pynutrien",
    packages=find_packages(
        include=["pynutrien", "pynutrien.*"]
    ),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/Nutrien/insights-framework",
    version="0.1.0",
    zip_safe=False,
)
