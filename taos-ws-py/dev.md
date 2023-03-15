# taos-ws-py

This is a Python websocket client for TDengine.

## Init

```bash

pip install maturin

```

## Installation

```bash
maturin build --strip && pip3 install ./target/wheels/taos_ws_py-0.2.3-cp37-abi3-macosx_10_7_x86_64.whl --force-reinstall
```

## Test

```bash

pytest tests

```

## Release

```bash

maturin build --release --future-incompat-report --strip
    
```