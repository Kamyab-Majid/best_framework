#!/usr/bin/core python

"""The setup script."""

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [
    # "Click>=7.0",
    # "awswrangler==2.17.0",
    # "aws-secretmanager-caching==1.1.1.5",
    # "hdbcli==2.14.22",
    # "redshift-connector==2.0.909"
]
test_requirements = [
    "pytest>=3",
]

setup(
    author="Insights Team",
    author_email="insights@nutrien.com",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    description="A shared python data processing framework",
    entry_points={},
    install_requires=requirements,
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="pynutrien",
    name="pynutrien",
    packages=find_packages(include=["pynutrien", "pynutrien.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/Nutrien/insights-framework",
    version="1.0.0",
    zip_safe=True,
)
