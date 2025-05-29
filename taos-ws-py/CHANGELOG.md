# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Conventional Changelog](https://www.conventionalcommits.org/en/v1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.5.2 - 2025-05-27

- **(taos-ws-py)**: Optimization of Python connector WebSocket poll

## v0.5.1 - 2025-05-16

### Features:

- **(taos-ws-py)**: support stmt2 in python websocket connector (#327)

## v0.4.0 - 2025-03-31

### Features:

- support dynamic add tmq attribute for taosws

## v0.3.9 - 2025-02-22

### Bug Fixes:

-  offset maybe euqal or over 0
-  poetry --with:dev compatible old programe
-  test_pandas is taospy test case
- **(ws)**:  fix cursor fetchmany

### Documents:

-  changelog rewrite

## v0.3.8 - 2025-01-02

### Features:

-  Supported Apache SuperSet with TDengine Cloud Data Source


## v0.3.3 - 2024-09-19

### Enhancements:

- add api committed, position
- update  cargo taos version
- **(ws)**: add tmq commit_offset
- **(ws)**: add tmq list_topics

### Documents:

- update dev cmd

## v0.3.2 - 2023-12-15

### Features:

- **(ws-py)**:  support RUST_LOG

### Bug Fixes:

-  add decorator
-  changelog
-  reformat lib
-  test case

### Enhancements:

-  update taos version
- **(ws-py)**:  update taos version

### Documents:

-  add set env cmd
-  update build cmd
-  update build version

## v0.3.2 - 2023-12-15

### Features:

- **(ws-py)**:  support RUST_LOG

### Bug Fixes:

-  add decorator
-  changelog
-  reformat lib
-  test case

### Enhancements:

-  update taos version
- **(ws-py)**:  update taos version

### Documents:

-  add set env cmd
-  update build cmd
-  update build version

## v0.3.2 - 2023-12-15

### Features:

- **(ws-py)**:  support RUST_LOG

### Bug Fixes:

-  add decorator
-  changelog
-  reformat lib
-  test case

### Enhancements:

-  update taos version
- **(ws-py)**:  update taos version

### Documents:

-  add set env cmd
-  update build cmd
-  update build version

## v0.3.2 - 2023-12-15

## v0.3.1 - 2023-10-08

### Bug Fixes:

- rust panic

## v0.3.0 - 2023-10-01

### Bug Fixes:

- update cargo version

## v0.2.9 - 2023-09-07

### Bug Fixes:

- fix fetch block via ws

## v0.2.8 - 2023-09-06

### Bug Fixes:

- return empty list when no data in cursor

## v0.2.7 - 2023-09-06

## v0.2.6 - 2023-09-05

### Bug Fixes:

- delete default host in taosws

## v0.2.5 - 2023-06-25

### Enhancements:

- add unsubscribe fn
- support assignment and seek via ws
- support schemaless via ws
- support stmt via ws

### Documents:

- update cmd

## v0.2.4 - 2023-03-29

## v0.2.4 - 2023-03-28

### Bug Fixes:

- add default port for taosrest sqlalchemy (#133)

### Enhancements:

- support req id ws (#161)
- TD-19401 support schemaless raw (#159)

## v0.2.3 - 2023-02-22

### Bug Fixes:

- fix rest and ws for cloud

