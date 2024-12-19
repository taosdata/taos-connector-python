# TDengine Connector for Python

| Github Workflow | PyPI Version | PyPI Downloads | CodeCov |
| --------------- | ------------ | -------------- | ------- |
| ![workflow](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-python/test-ubuntu-2204.yml) | ![PyPI](https://img.shields.io/pypi/v/taospy) | ![PyPI](https://img.shields.io/pypi/dm/taospy) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-python/branch/main/graph/badge.svg?token=BDANN3DBXS)](https://codecov.io/gh/taosdata/taos-connector-python) |




[TDengine](https://github.com/taosdata/TDengine) connector for Python enables python programs to access TDengine, using
an API which is compliant with the Python DB API 2.0 (PEP-249). It contains two modules:

1. The `taos` module. It uses TDengine C client library for client server communications.
2. The `taosrest` module. It wraps TDengine RESTful API to Python DB API 2.0 (PEP-249). With this module, you do not need to install the TDengine C client library.

## Install taospy

You can use `pip` to install the connector from PyPI:

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

## Docs

[Reference](https://docs.tdengine.com/reference/connector/python/)

## Limitation

- `taosrest` is designed to use with taosAdapter. If your TDengine version is older than v2.4.0.0, taosAdapter may not
  be available.

## License

We use MIT license for Python connector.
