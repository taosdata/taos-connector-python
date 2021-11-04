# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Conventional Changelog](https://www.conventionalcommits.org/en/v1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v2.1.2 - 2021-11-04

### Bug Fixes:

- [TD-10828]:fix import taos error for TDengine 2.2 or 2.0
- [TD-10838]:fix ci error in release

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

