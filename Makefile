.PHONY: clean clean-build clean-pyc clean-test coverage dist docs help install lint lint/flake8 lint/black
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint/autopep8: ## check style with flake8
	autopep8 pynutrien tests --diff -r

lint/black: ## check style with black
	black --check pynutrien tests

lint/isort: ## check style with isort
	isort --check --diff pynutrien/ tests/

lint: lint/isort lint/autopep8 lint/black  ## check style

format/autopep8: ## format style with autopep8
	autopep8 pynutrien tests -r --in-place

format/black: ## format style with black
	black pynutrien tests

format/isort: ## format style with isort
	isort pynutrien tests

format: format/autopep8 format/isort format/black ## format style


test: ## run tests quickly with the default Python
	pytest

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run --source pynutrien -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

Security: ## checks for security in dependencies and current package
	safety check -i 39462 -i 40291
	bandit -r pynutrien/

docs: ## generate Sphinx documentation using doc_type, including API docs.
	rm -f -r sphinx/_build
	rm -f -r docs
	rm -f sphinx/pynutrien.rst
	rm -f sphinx/modules.rst
	sphinx-apidoc -f -o sphinx/ pynutrien
	$(MAKE) -C sphinx clean
	$(MAKE) -C sphinx $(doc_type)
	$(BROWSER) docs/_build/html/index.html
	mv docs/$(doc_type) .
	rm -r docs
	mv $(doc_type) docs
	touch docs/.nojekyll

servedocs: docs ## compile the docs watching for changes. Usage:  use it whenever you have a server that wants to update as soon as the file changes. The command should be always running.
	watchmedo shell-command -p '*.rst;*.md' -c '$(MAKE) -C docs $(doc_type)' -R -D .



release: dist glue_zip lambda_zip ## package and upload a release
	twine upload dist/*

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	python setup.py install
pause: ##pause for 10 seconds
	sleep 1
done: test pause test-all pause security pause docs doc_type=html lint release

glue_zip:
	mkdir -p wheel_dir/python
	find dist -name "*.whl" -print0 | xargs -0 -I {} cp {} wheel_dir/python
	pip wheel --wheel-dir=wheel_dir/python -r glue_requirements.txt
	for z in wheel_dir/python/*.whl; do unzip $$z -d wheel_dir/python; done
	find wheel_dir/python -type f -iname "*.whl" -exec rm -rf {} +
	cd wheel_dir/python/; zip glue_zip * -r
	mv wheel_dir/python/glue_zip.zip dist/glue_zip.zip
	find wheel_dir/python -type d -iname "*" -exec rm -rf {} +
	rm -r -f wheel_dir
lambda_zip:
	mkdir -p wheel_dir/python
	find dist -name "*.whl" -print0 | xargs -0 -I {} cp {} wheel_dir/python
	pip wheel --wheel-dir=wheel_dir/python -r lambda_requirements.txt
	for z in wheel_dir/python/*.whl; do unzip $$z -d wheel_dir/python; done
	find wheel_dir/python -type f -iname "*.whl" -exec rm -rf {} +
	cd wheel_dir/; zip lambda_layer * -r
	mv wheel_dir/lambda_layer.zip dist/lambda_layer.zip
	find wheel_dir/python -type d -iname "*" -exec rm -rf {} +
	rm -r -f wheel_dir
