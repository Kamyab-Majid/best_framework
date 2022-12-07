# Makefile Documentation

In this documentation, we are going to briefly describe what each makefile command does.

## Introduction

To briefly describe the commands, please use `make help` in the terminal of your choice which will generate:

- **clean**:                remove all build, test, coverage and Python artifacts
- **clean-build**:          remove build artifacts.
- **clean-pyc**:            remove Python file artifacts.
- **clean-test**:           remove test and coverage artifacts.
- **lint**:                 check style.
- **format**:               format style.
- **test**:                 run tests quickly with the default Python.
- **test-all**:             run tests on every Python version with tox.
- **coverage**:             check code coverage quickly with the default Python.
- **docs**:                 generate Sphinx documentation using doc_type, including API docs.
- **servedocs**:            compile the docs watching for changes. Usage:  use it whenever you have a server that wants to update as soon as the file changes. The command should be always running.
- **release**:              package and upload a release
- **dist**:                 builds source and wheel package
- **install**:              install the package to the active Python's site-packages
- **Security**             checks for security in dependencies and current package
- **bump**             bumps the version given the version argument that can be patch, minor or major for more information visit [bumpversion](https://pypi.org/project/bumpversion/)
- **glue_zip**              creates a lambda zip file from the glue_requirements.txt and current pynutrien version.
- **lambda_zip**           creates a lambda zip file from the lambda_requirements.txt and current pynutrien version.

In the next sections, we are going to briefly describe some commands in more detail.

## Dist

Create the [distribution](https://docs.python.org/3/library/distribution.html) for the current python package. To build distribution package use this option. This will generate a distribution archives in the dist folder. In fact, if you look in your directory you should see a few new folders one called dist and one called build. These were generated when we ran the command.

## Install

install the package to the active Python's environment site-packages locally.

## Clean

It will clean the dist folder which include [distribution](https://packaging.python.org/en/latest/glossary/)

## Test

Test the package using [pytest.py](https://docs.pytest.org/en/7.1.x/).

## test-all

It is a more general way of testing the package with multiple python versions using [tox](https://tox.wiki/en/latest/)

## Coverage

It examines the package to see if a part of the code is missed while testing using [coverage](https://coverage.readthedocs.io/en/6.5.0/) package. This is used to make sure the tests are inclusive of every line of the codes.

## Lint

check the style of each python file to make sure it conforms with the expected formats of [autopep8](https://pypi.org/project/autopep8/), [black](https://black.readthedocs.io/en/stable/) and [isort](https://pypi.org/project/isort/). This is a good practice to do before releasing the package.

## Format

formats the the style of each python file to make sure it conforms with the expected formats of [autopep8](https://pypi.org/project/autopep8/), [black](https://black.readthedocs.io/en/stable/) and [isort](https://pypi.org/project/isort/).

## Security

This option is check the libraries with [safety](https://pypi.org/project/safety/) and the code in this project with [Bandit](https://pypi.org/project/bandit/).
Safety has a database in which packages with security issues using [CVE](https://www.cvedetails.com/index.php). By searching between your environment packages it will try to find security issues considering the version of those packages. You can see a full report using `safety check --full-report`.
Bandit on the other hand, search between your code and find security issues such as a password or a token inside your code.

## Release

package and upload a release of the package. Make sure to update setup.py config beforehand using [python documentation on setup.py](https://docs.python.org/3/distutils/setupscript.html).
In order to use make to release a version, This is the proposed procedure:

```sh
make test
make test-all
make coverage
make security
make format
make lint
make release
```