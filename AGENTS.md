# AGENTS.md — taos-connector-python

Think in English, but always provide your final response in Chinese.

## Project Overview

This is the official Python connector for TDengine (a time-series database). The repo ships two PyPI packages from a single codebase:

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
- SQLAlchemy dialect ownership (current state): `TaosDialect` and `TaosWsDialect` are both implemented in `taos/sqlalchemy.py`, while `TaosRestDialect` is in `taosrest/sqlalchemy.py`.
- SQLAlchemy entry points are currently registered in root `pyproject.toml` (`taos`, `taosrest`, `taosws`).

## Directory Structure Guidance

Current baseline (no mandatory restructure): keep the current top-level layout for regular fixes/features.

- `taos/` (native + shared SQLAlchemy dialect code)
- `taosrest/` (REST DB-API and REST SQLAlchemy dialect)
- `taos-ws-py/` (Rust/PyO3 WebSocket connector)
- `tests/` and `taos-ws-py/tests/` (split test suites)

## Build & Dev Setup

Run all build/format/test/release commands in a conda virtual environment first:

```bash
conda activate base
```

### taospy (taos + taosrest)

```bash
conda activate base
pip install poetry==1.8.5
poetry install --no-interaction --with=dev
```

### taos-ws-py (taosws)

Install WebSocket package into the same Poetry environment when you need taosws runtime/tests:

```bash
conda activate base
pip install taos-ws-py
```

When you modify Rust code under `taos-ws-py/`, build and install the local wheel for validation:

```bash
conda activate base
cd taos-ws-py
python3 -m maturin build --strip
pip3 install ./target/wheels/<generated-wheel>.whl --force-reinstall
```

### Formatting

For Python code in this repo (taospy and shared Python files), use Black with **line-length 119**:
```bash
conda activate base
poetry run black --check .
```

For Rust code in `taos-ws-py/`, use rustfmt:
```bash
conda activate base
cd taos-ws-py && cargo fmt --all --check
```

Pre-commit hooks also run `typos` for spell-checking and yaml/json validation.

## Testing

Tests require a **running TDengine server**. Most tests connect to a live instance.

### taospy tests (`tests/`)

```bash
conda activate base
# Set the REST API endpoint (required for REST tests)
export TDENGINE_URL=localhost:6041

# Run all taospy/taosrest tests
poetry run pytest tests/

# Run a single test file
poetry run pytest tests/test_connection.py

# Run a single test function
poetry run pytest tests/test_connection.py::test_default_connect
```

`tests/test_sqlalchemy.py` includes `taos://`, `taosrest://`, and `taosws://` coverage in current main branch.

### taos-ws-py tests (`taos-ws-py/tests/`)

```bash
conda activate base
# WebSocket package tests
poetry run pytest taos-ws-py/tests/
```

Quick scripts:

```bash
conda activate base
bash ./test_taospy.sh
bash ./test_taos-ws-py.sh
```

`tests/decorators.py` provides a `@check_env` decorator that skips tests when `TDENGINE_URL` is not set.

## Conventions

- **taospy conventions (`taos`, `taosrest`)**:
	- PEP 249 (DB-API 2.0): `connect()`, `Connection`, `Cursor`
	- Error trees: `taos/error.py` and `taosrest/errors.py` (hex code format: `"[0x%04x]: %s"`)
	- Version source: `taos/_version.py`
	- `IS_V3` guard is widely used for v2/v3 behavior branches
- **taos-ws-py conventions (`taosws`)**:
	- Version source: `taos-ws-py/Cargo.toml`
	- Rust + PyO3 project under `taos-ws-py/`, tested separately in `taos-ws-py/tests/`
- **Shared defaults**:
	- Default credentials: `user="root"`, `password="taosdata"`
	- Release scripts: `ci/release.sh <version>` (taospy), `ci/release-ws.sh <version>` (taos-ws-py)

## Modifying Packages

### taospy (`taos`, `taosrest`)

Most taospy changes are in Python modules under `taos/`, `taosrest/`, and `tests/`.

- Use Poetry-managed Python tooling for formatting and tests
- Keep PEP 249 behavior consistent across `taos` and `taosrest`
- Validate with taospy tests in `tests/`
- Note: `TaosWsDialect` currently also lives in `taos/sqlalchemy.py`, so taosws SQLAlchemy changes are made in taospy code today

### taos-ws-py (`taosws`)

The `taos-ws-py/` directory is a Rust project using PyO3. It depends on the `taos` Rust crate from `taos-connector-rust`.

- Changes require a Rust toolchain
- Build with `maturin` or `cargo build`
- Validate with tests in `taos-ws-py/tests/`
- If the change impacts SQLAlchemy behavior for `taosws://`, also verify `tests/test_sqlalchemy.py`

## Documentation Sync Checklist

When changing packaging, SQLAlchemy ownership, or test boundaries, update all relevant docs in the same PR:

- `AGENTS.md` (developer workflow + ownership)
- `README.md` and `README-CN.md` (user-facing install/use paths)
- `taos-ws-py/README.md` and `taos-ws-py/dev.md` (taosws contributor workflow)
- `docs/superpowers/specs/*.md` (design/spec status and rollout notes)
