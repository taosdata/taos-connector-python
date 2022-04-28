# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Conventional Changelog](https://www.conventionalcommits.org/en/v1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v2.3.0 - 2022-04-28

### Features:

- add  RestClient
- implement PEP249 Connections and Cursor API
- remove wsclient and refactor folder structure
- **(rest)**: adapter code for sqlalchemy and pandas
- **(restful)**: add some test cases
- **(rest)**: increase version number
- support 3.0

### Bug Fixes:

- cannot iter on TaosFields twice
- catch datetime overflow and pass
- decrease version number
- failed to convert rfc3339 format time string
- fix pytest error for v3 check
- fix set_tz function not found
- float is_null missed
- remove buggy line in taos_fetch_row
- restore taos_options
- rowcount is -1 for INSERT statement
- taos_query() result for 3.0
- typo
- typos
- use isnan() or is_null for float/double
- use taos_is_null()

### Enhancements:

- add time utile to parse rfc3339 format time string
- ident json
- **(RestClient)**: support timeout parameter
- **(rest)**: convert timestamp from 'str' to 'datetime'
- **(rest)**: export RestClient
- **(taos)**: make config method private
- **(taos)**: reanme _config to _init_config
- use iso8601 to parse date

### Tests:

- add test cases for pandas
- test rowcount

### Documents:

- add docs to cursor.description
- add limitation of taosrest
- fix typo and align parameters order
- schemaless docs
- update manual
- update readme for taosres
- update user manual

## v2.2.5 - 2022-04-13

### Features:

- [TD-14696]:support timezone option when connect

## v2.2.4 - 2022-03-31

### Bug Fixes:

- [TD-14410]:add affected_rows property for stmt
- [TD-14410]:fix import error in taos 2.0/2.2
- [TD-14410]:use stmt.affected_rows in examples

## v2.2.3 - 2022-03-29

### Bug Fixes:

- [TD-14371]:remove unecessary print() lines
- [TD-14382]:fix row_count property always be 0

## v2.2.2 - 2022-03-28

### Features:

- [TD-14210]:support sqlalchemy dialect plugin

### Bug Fixes:

- [TD-14358]:remove stream api

## v2.2.1 - 2022-01-12

### Bug Fixes:

- [TS-1107]:use typing as one of dev-dependencies

## v2.2.0 - 2021-12-23

### Bug Fixes:

- [TD-10875]:test for each git push
- [TD-12253]:fix syntax error in README

### Tests:

- [TD-10875]:add GitHub Actions test workflow on pr

### Documents:

- [TD-12250]:fix README confused titles

## v2.1.2 - 2021-11-05

### Bug Fixes:

- [TD-10828]:fix import taos error for TDengine 2.2 or 2.0
- [TD-10838]:fix ci error in release

### Documents:

- [TD-10828]:improve document for pip install

## v0.1.0 - 2021-09-09

### Features:

- [TD-2971]:make python connector support unsigned int. fix None determine.
- [TD-3048]:support lines/stream/query_a/stop_query/ and so on. (#7079)
- [TD-4647]:auto add column through schemaless line protocol
- [TD-4752]:python connector support nanosecond. (#6528)

### Bug Fixes:

- [TD-2915]:python connector cursor iter next() function. (#5875)
- [TD-3288]:solve python connector bigint and timestamp issue on 32bit.
- [TD-4048]:fix python connector error where ts is null (#6018)
- [TD-4160]:remove python connector soft links, fix tests and documents
- [TD-4370]:squashed commit of python connector changes in develop (#6246)
- [TD-4640]:fix CDLL error in macOs (#6430)
- [TD-4808]:fix python connector error where ts is null (#6025)
- [TD-5585]:arm32 python fromtimestamp error. (#7090)
- [TD-5892]:publish python connector to PyPI
- [TD-6169]:windows dll client can not quit.
- [TD-6231]:fix all none error of stmt multibind in python connector (#7482)
- [TD-6241]:fix unexpected nan while inserting None as float/doub… (#7486)
- [TD-6313]:improve error handling if loading taos failed in python (#7542)
- [TD-6313]:improve error handling if loading taos failed in python (#7550)

### Enhancements:

- [TD-182]:use single repo for python connector (#6036)

### Documents:

- [TD-4803]:fix repo name in python connector README (#6559)
- [TD-6449]:prefer english only in method documentations in python connector [ci skip] (#7711)

