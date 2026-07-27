# Pin macOS CI to Intel Runner

## Problem

The `Test on macOS` workflow uses `macos-latest` for all three jobs. GitHub now resolves that label to a
macOS 26 arm64 runner with AppleClang 21. TDengine and some of its bundled dependencies use deprecated
literal-operator syntax, and their builds promote the resulting compiler warnings to errors. Consequently,
the `install TDengine` step fails before the connector can be built or tested.

## Design

Change the `runs-on` value for the `build-tdengine`, `build-poetry`, and `test` jobs from `macos-latest` to
`macos-15-intel`. Keeping all three jobs on the same runner architecture ensures that the packaged TDengine
executables and dynamic libraries are compatible with the Python build and test processes that consume them.

Do not change TDengine source code, compiler warning policy, CMake arguments, cache keys, or other workflows.
The existing `taos-ws-py` workflow already uses `macos-15-intel`, so the label is an established repository
convention.

## Verification

Validate the workflow as YAML, run an available GitHub Actions static checker, and inspect the final diff to
confirm that exactly three runner labels changed. A local Linux host cannot reproduce the hosted macOS runner,
so the PR's `Test on macOS` workflow remains the end-to-end verification.

