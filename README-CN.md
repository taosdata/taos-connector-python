# TDengine Python Connector

| Github Workflow | PyPI Version | PyPI Downloads | CodeCov |
| --------------- | ------------ | -------------- | ------- |
| ![workflow](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-python/test-ubuntu-2204.yml) | ![PyPI](https://img.shields.io/pypi/v/taospy) | ![PyPI](https://img.shields.io/pypi/dm/taospy) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-python/branch/main/graph/badge.svg?token=BDANN3DBXS)](https://codecov.io/gh/taosdata/taos-connector-python) |

English | [简体中文](./README-CN.md)

## 1. Introduction
[TDengine](https://github.com/taosdata/TDengine) connector for Python enables python programs to access TDengine, using
an API which is compliant with the Python DB API 2.0 (PEP-249). It contains two modules:

1. The `taos` module. It uses TDengine C client library for client server communications.
2. The `taosrest` module. It wraps TDengine RESTful API to Python DB API 2.0 (PEP-249). With this module, you do not need to install the TDengine C client library.


## 2. Get the Driver

## Install taospy

You can use `pip3` to install the connector from PyPI:

```bash
pip3 install taospy
```

Or with git url:

```bash
pip3 install git+https://github.com/taosdata/taos-connector-python.git
```

Note: taospy v2.7.2 requirs Python 3.6+. The early versions of taospy from v2.5.0 to v2.7.1 require Python 3.7+.

## Install taos-ws-py (Support WebSocket)

```bash
# taos-ws-py depends taospy
pip3 install taospy
pip3 install taos-ws-py
```

Note: The taosws module is provided by taos-ws-py package separately from v2.7.2. It is part of early version of taospy.
taos-ws-py requires Python 3.7+.


## 3. Documentation  
- For development examples, see [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes how an application can introduce the `taos-connector-python`, as well as examples of data writing, querying, schemaless writing, parameter binding, and data subscription.
- For other reference information, see [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/python/), which includes version history, data types, example programs, API descriptions, and FAQs.
- To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 4. Limitation

- `taosrest` is designed to use with taosAdapter. If your TDengine version is older than v2.4.0.0, taosAdapter may not
  be available.


## 5. Prerequisites

### For taospy

1.  `TDengine` enviroment, install refer to [Here](https://www.taosdata.com/) 
2.  `Python3` enviroment, install refer to [Here](https://www.python.org/)

### For taos-ws-py

1.  `TDengine` enviroment, install refer to [Here](https://www.taosdata.com/) 
2.  `Python3` enviroment, install refer to [Here](https://www.python.org/)
3.  `Rust` build enviroment, install refer to [Here](https://www.rust-lang.org/learn/get-started)
4.  Install `maturin` with `pip3 install maturin`


## 6. Build

Download the repository code and execute the following in root directory:
``` bash
pip3 install ./ 
```
or install in editable mode (i.e. "develop mode") 
``` bash
pip3 install -e ./ 
```

## 7. Testing
### 7.1 Test Execution

python 连接器测试使用的是 `pytest` 测试框架编写的测试用例，使用 `pytest` 测试即可
taospy 组件测试用例目录在根目录下的 tests/
taos-ws-py 组件测试用例目录在根目录下的 taos-ws-py/tests/ 

``` bash
# taospy
cd tests
pytest ./ 

# taos-ws-py
cd taos-ws-py/tests/ 
pytest ./ 
```

### 7.2 Test Case Addition
All tests are located in the `tests/` directory of the project. The directory is divided according to the functions being tested. You can add new test files or add test cases in existing test files.
The test cases use the JUnit framework. Generally, a connection is established and a database is created in the `before` method, and the database is droped and the connection is released in the `after` method.

### 7.3 Performance Testing
Performance testing is in progress.

## 8. CI/CD
- [Build Workflow](https://github.com/taosdata/taos-connector-python/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-python)

## 9. Submitting Issues
We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-jdbc/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Problem description, whether it always occurs, and it's best to include a detailed call stack.
- JDBC driver version.
- JDBC connection parameters (username and password not required).
- TDengine server version.

## 10. Submitting PRs
We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
1. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
1. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
1. Push the changes to the remote branch (`git push origin my_branch`).
1. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
1. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-jdbc/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
1. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-jdbc/pulls) page to check the test coverage.

## 11. References
- [TDengine Official Website](https://www.tdengine.com/) 
- [TDengine GitHub](https://github.com/taosdata/TDengine) 

## 12. License
[MIT License](./LICENSE)