# Coding Documentation

This document lay outs certain practices for Adastra developers to follow in the Python language.This coding standards documentation serves the following purposes

## Best Practices

## Code Quality

### Linters

Linters use source code analysis to detect programming errors, bugs, stylistic errors, and suspicious constructs. Linting tools are simple to use, provide reasonable defaults, and improve the overall developer experience by removing friction between developers who disagree on style.
Here we discuss some of them:

#### Flake8

Flake8 is a wrapper around [Pyflakes](https://github.com/PyCQA/pyflakes), [pycodestyle](https://github.com/pycqa/pycodestyle), and [McCabe](https://github.com/PyCQA/mccabe) which will do the following linting:

- **code logic**:- these check for programming errors, enforce code standards, search for code smells, and check code complexity. Pyflakes and McCabe (complexity checker) are the most popular tools for linting code logic.
-**code style** - these just enforce code standards (based on PEP-8). pycodestyle falls into this category.

### Code Formatting

Consider using auto format on save to easily format your codes and documents [^1].
[^1]: If you are using VSCode follow [VScode format on autosave](https://blog.yogeshchavan.dev/automatically-format-code-on-file-save-in-visual-studio-code-using-prettier#automatically-format-code-on-file-save.), in case you are using Pycharm take a look at [Pycharm format on autosave](https://www.jetbrains.com/help/pycharm/reformat-and-rearrange-code.html#reformat-on-save).
