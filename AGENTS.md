# AGENTS.md — taos-connector-python

Think in English, but always provide your final response in Chinese.

## Project Overview

This is **taospy**, the official Python connector for TDengine (a time-series database). The repo ships two PyPI packages from a single codebase:

- **taospy** (modules: `taos` + `taosrest`) — native C and REST connectors
- **taos-ws-py** (module: `taosws`) — WebSocket connector built in Rust via PyO3

## Architecture — Three Connection Modes

| Package | Module | Transport | Dependency |
|---------|--------|-----------|------------|
| taospy | `taos` | Native C FFI (`ctypes` → `libtaos.so`) | TDengine client library installed on host |
| taospy | `taosrest` | HTTP REST API (`requests`) | Running taosAdapter (default `localhost:6041`) |
| taos-ws-py | `taosws` | WebSocket (Rust → PyO3) | Running taosAdapter |

**Key relationships:**
- `taos/cinterface.py` loads the native C library and wraps all C functions via `ctypes`. All native-mode classes depend on it.
- `taosrest/restclient.py` wraps the TDengine REST API. `taosrest/connection.py` and `taosrest/cursor.py` implement PEP 249 on top.
- `taos-ws-py/` is a standalone Rust crate (PyO3) that compiles to a `taosws` Python module. It has its own `Cargo.toml`, tests, and release cycle.
- All three modes register SQLAlchemy dialects: `taos://`, `taosrest://`, `taosws://` (see `taos/sqlalchemy.py`, `taosrest/sqlalchemy.py`).

## Build & Dev Setup

```bash
pip install poetry==1.8.5
poetry install --no-interaction --with=dev
```

For WebSocket support: `poetry run pip install taos-ws-py`

### Formatting

Black with **line-length 119**:
```bash
poetry run black --check .
```

Pre-commit hooks also run `typos` for spell-checking and yaml/json validation.

## Testing

Tests require a **running TDengine server**. Most tests connect to a live instance.

```bash
# Set the REST API endpoint (required for REST tests)
export TDENGINE_URL=localhost:6041

# Run all tests
poetry run pytest tests/

# Run a single test file
poetry run pytest tests/test_connection.py

# Run a single test function
poetry run pytest tests/test_connection.py::test_default_connect

# WebSocket tests (separate directory)
poetry run pytest taos-ws-py/tests/
```

`tests/decorators.py` provides a `@check_env` decorator that skips tests when `TDENGINE_URL` is not set.

## Conventions

- **PEP 249 (DB-API 2.0)**: Both `taos` and `taosrest` implement the Python DB-API 2.0 spec — `connect()`, `Connection`, `Cursor` pattern.
- **Error hierarchy**: `taos/error.py` and `taosrest/errors.py` each define their own PEP 249-aligned exception trees rooted at `Error`. Errors include a hex error code: `"[0x%04x]: %s"`.
- **Version management**: `taos/_version.py` holds the taospy version. `taos-ws-py/Cargo.toml` holds the taos-ws-py version. They are versioned independently.
- **IS_V3 guard**: Many tests and features check `taos.IS_V3` to branch between TDengine v2 and v3 behavior.
- **Default credentials**: `user="root"`, `password="taosdata"` throughout.
- **Release**: `ci/release.sh <version>` for taospy, `ci/release-ws.sh <version>` for taos-ws-py. These generate changelogs, bump versions, tag, and push.

## Modifying taos-ws-py

The `taos-ws-py/` directory is a Rust project using PyO3. It depends on the `taos` Rust crate from `taos-connector-rust`. Changes require a Rust toolchain and `maturin` or `cargo build`. This sub-project has its own tests under `taos-ws-py/tests/`.
