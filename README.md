
## Data Processing Framework (DPF)

The framework is intended to facilitate development for creating and deploying AWS services and pipelines. The DPF has 3 components.

1) A [common/shared python library](https://github.com/Nutrien/insights-framework) (this repo)
    - Contains global python functionality and cross-service connectivity
    - Adds utility functions for development
    - Generic portions of new projects should make contributions here for reuse.
    - Dependency resolution/management
2) A [DevOps Library](https://github.com/Nutrien/insights-devops/tree/develop)
    - Service/Component level templates for deployment
3) Your project repository, which defines your tasks/services that uses the above as it's building blocks.

This project was built and tested with *Python 3.7*


### Project Quick-Start

In a new project, you will want to include this library as a dependency.

You can download the latest python build [v1.0.0-alpha](https://github.com/Nutrien/insights-framework/blob/release/v1.0.0-alpha/dist/pynutrien-1.0.0-py2.py3-none-any.whl), and include it in a `lib` folder in your project. Or setup a [Virtual Environment](https://docs.python.org/3.7/library/venv.html) using `pip install lib/pynutrien-1.0.0-py2.py3-none-any.whl`

In the root of your project

```sh
# Create a Virtual Environment
python3.7 -m venv venv

# Activate the environment
source ./venv/bin/activate
# Windows: venv\Scripts\activate.bat

# Install requirements (or use docker image)
pip install lib/pynutrien-1.0.0-py2.py3-none-any.whl

```

You will have to include [`PyGlue.zip`](https://s3.amazonaws.com/aws-glue-jes-prod-us-east-1-assets/etl/python/PyGlue.zip) in a lib folder or into your venv.
See [AWS instructions on PyCharm Setup](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-pycharm.html)



### Library Structure

- `pynutrien` folder, there are 4 main sub-packages provided by this library.
    - [`pynutrien.core`](https://redesigned-pancake-2843d68c.pages.github.io/pynutrien/pynutrien.aws.html)
        - Common python utilities (logging, configs, etc.)
    - [`pynutrien.aws`](https://redesigned-pancake-2843d68c.pages.github.io/pynutrien/pynutrien.core.html)
        - AWS Service level utilities
    - [`pynutrien.etl`](https://redesigned-pancake-2843d68c.pages.github.io/pynutrien/pynutrien.etl.html)
        - ETL Helper utilities
        - Does not have PySpark as a dependency
    - [`pynutrien.util`](https://redesigned-pancake-2843d68c.pages.github.io/pynutrien/pynutrien.util.html)
        - Other helper utilities
        - Mostly self-contained utilities
- `devops` sub-module link to the DevOps Repository
- `docs` source folder for `sphinx` and building docs
- `util` folder with shell scripts
- `tests` which contains the unit tests
- `jupyter_workspace` contains example/template notebooks

For more information see the [API Reference](https://redesigned-pancake-2843d68c.pages.github.io/pynutrien/modules.html)

Dependency graphs (generated with [`pydeps`](https://github.com/thebjorn/pydeps)):

- [Simplified](https://github.com/Nutrien/insights-framework/tree/develop/docs/src/_static/pictures/deps/deps1.svg)
- [Full](https://github.com/Nutrien/insights-framework/tree/develop/docs/src/_static/pictures/deps/deps2.svg)
- [Grouped](https://github.com/Nutrien/insights-framework/tree/develop/docs/src/_static/pictures/deps/deps3.svg)

### Docker

Included are a lambda and glue development Dockerfile to build an image.

The `util` folder has some built-in scripts for running/testing ETL based jobs for Glue/EMR.

### Coding / Style

To correct most stylistic issues

```sh
make format
```

Which will currently use
- [autopep8](https://github.com/hhatto/autopep8)
- [isort](https://pycqa.github.io/isort/)
- [black](https://black.readthedocs.io/en/stable/)

Noteworthy configuration changes are:
- Max line length set to 120 characters
- Max function/method (McCabe complexity)[https://en.wikipedia.org/wiki/Cyclomatic_complexity] >10
    - briefly: no more than 10 branching paths inside one function.

To check other style compliance issues.

```sh
make lint
```


Linting issues and codes can be found in the respective linting library documentation.

More commands in the [Makefile Documentation](https://redesigned-pancake-2843d68c.pages.github.io/makefile.html).


### Notebooks

See the [Jupyter Workspace](https://github.com/Nutrien/insights-framework/tree/develop/jupyter_workspace) for a template and feature demo.

### Deployment

This project build will create these artifacts:

- A documentation build
- Various test/coverage reports
- A python wheel
- A zip with all the dependencies for glue
- A zip with all the dependencies for lambda (assuming using layer for `awswrangler`)

### More Information

View the [Library Documentation](https://redesigned-pancake-2843d68c.pages.github.io/) and [Confluence Page](https://nutrien.atlassian.net/wiki/spaces/DSIPT/pages/187957249/Data+Processing+Framework)

