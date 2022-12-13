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
    install_requires=[
        "boto3==1.20.32",
        "botocore==1.23.32",
        "certifi==2020.11.8",
        "chardet==4.0.0",
        "charset-normalizer==2.0.12",
        "idna==2.10",
        "jmespath==0.10.0",
        "python-dateutil==2.8.2",
        "requests==2.26.0",
        "s3transfer==0.5.1",
        "setuptools==47.1.0",
        "six==1.16.0",
        "urllib3==1.26.6",
        "awswrangler==2.16.1",
        "pandas==1.5.2",
        "pydevd-pycharm==223.8214.17",
        "pytz==2022.6",
        "pyYAML==6.0",
        "s3fs==0.4.2",
        "tomli==2.0.1",
        "aws-secretsmanager-caching==1.1.1.5",
        "python-dotenv==0.21.0",
        "dictmerge==0.2.0",
        "pyspark==3.3.1",
        "wheel==0.38.4",
        "hdbcli==2.14.22",
        "redshift-connector==2.0.909"],
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
