<!-- omit in toc -->
# TDengine Python Connector

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-python/test-ubuntu-2204.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-python/branch/main/graph/badge.svg?token=BDANN3DBXS)](https://codecov.io/gh/taosdata/taos-connector-python)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-python)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-python)
![PyPI](https://img.shields.io/pypi/v/taospy)
![PyPI](https://img.shields.io/pypi/dm/taospy) 
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](./README-CN.md)

<!-- omit in toc -->
## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
  - [For taospy](#for-taospy)
  - [For taos-ws-py](#for-taos-ws-py)
- [6. Build](#6-build)
- [7. Testing](#7-testing)
  - [7.1 Test Execution](#71-test-execution)
  - [7.2 Test Case Addition](#72-test-case-addition)
  - [7.3 Performance Testing](#73-performance-testing)
- [8. CI/CD](#8-cicd)
- [9. Submitting Issues](#9-submitting-issues)
- [10. Submitting PRs](#10-submitting-prs)
- [11. References](#11-references)
- [12. License](#12-license)

## 1. Introduction

`taospy` is the official Python Connector for TDengine, allowing Python developers to develop applications that access the TDengine database. It supports functions such as data writing, querying, subscription, schemaless writing, and parameter binding.

The API for `taospy` is compliant with the Python DB API 2.0 (PEP-249). It contains two modules:

1. The `taos` module. It uses TDengine C client library for client server communications.
2. The `taosrest` module. It wraps TDengine RESTful API to Python DB API 2.0 (PEP-249). With this module, you do not need to install the TDengine C client library.

## 2. Documentation

- To use Python Connector, please check [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes how an application can introduce the Python Connector , as well as examples of data writing, querying, schemaless writing, parameter binding, and data subscription.
- For other reference information, please check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/python/), which includes version history, data types, example programs, API descriptions, and FAQs.
- This quick guide is mainly for developers who like to contribute/build/test the Python Connector by themselves. To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

### For taospy

- Python 3.6.2 or above runtime environment
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been started.


### For taos-ws-py

- Python 3.7 or above runtime environment
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been started.


## 6. Build

Download the repository code and execute the following in root directory to build develop environment:
``` bash
pip3 install ./ 
```
or install in editable mode (i.e. "develop mode") 
``` bash
pip3 install -e ./ 
```

## 7. Testing
### 7.1 Test Execution
The Python Connector testing framework is `pytest`  
The testing directory for `taospy` is located in the root directory: ./tests/  
The testing directory for `taos-ws-py` is located in the root directory: ./taos-ws-py/tests/  

The following command runs all test cases:
``` bash
# for taospy
pytest ./tests

# for taos-ws-py
pytest ./taos-ws-py/tests/
```

### 7.2 Test Case Addition
You can add new test files or add test cases in existing test files that comply with `pytest` standards

### 7.3 Performance Testing
Performance testing is in progress.

## 8. CI/CD
- [Build Workflow](https://github.com/taosdata/taos-connector-python/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-python)

## 9. Submitting Issues
We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-python/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Problem description, whether it always occurs, and it's best to include a detailed call stack.
- Python Connector version.
- Python Connection parameters (username and password not required).
- TDengine server version.

## 10. Submitting PRs
We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-python/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-python/pulls) page to check the test coverage.

## 11. References
- [TDengine Official Website](https://www.tdengine.com/) 
- [TDengine GitHub](https://github.com/taosdata/TDengine) 

## 12. License
[MIT License](./LICENSE)