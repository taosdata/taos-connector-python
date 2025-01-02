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

[Reference](https://docs.tdengine.com/tdengine-reference/client-libraries/python/)

## Limitation

- `taosrest` is designed to use with taosAdapter. If your TDengine version is older than v2.4.0.0, taosAdapter may not
  be available.

## License

We use MIT license for Python connector.

## Contributing

### For taospy

**Precondictions**
1.  `TDengine` enviroment, install refer to [Here](https://www.taosdata.com/) 
2.  `Python3` enviroment, install refer to [Here](https://www.python.org/)
**Building & Install**
Download the repository code and execute the following in root directory:
``` bash
pip3 install ./ 
```

or install in editable mode (i.e. "develop mode") 
``` bash
pip3 install -e ./ 
```

**Testing**
 Refer to the examples in ./tests/ 


### For taos-ws-py
**Precondictions**
1.  `TDengine` enviroment, install refer to [Here](https://www.taosdata.com/) 
2.  `Python3` enviroment, install refer to [Here](https://www.python.org/)
3.  `Rust` build enviroment, install refer to [Here](https://www.rust-lang.org/learn/get-started)
4.  Install `maturin` with `pip3 install maturin`
**Building & Install**
Download the repository code and execute the following in root directory:
```bash
cd taos-ws-py
python3 -m maturin build --strip
# repalce xxx with real generated filename
pip3 install ./target/wheels/taos_ws_py-xxx.whl
```
**Testing**
 Refer to the examples in ./tests/ 