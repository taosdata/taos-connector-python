# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import ctypes
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytz
import pytest
from tzlocal.windows_tz import win_tz

from taos import bind2, cinterface, field
from taos.bind_base import datetime_to_timestamp
from taos.connection import TaosConnection
from taos.constants import FieldType
from taos.cursor import TaosCursor
from taos.field import DEFAULT_TIMEZONE
from taos.precision import PrecisionEnum
from taos.statement import TaosStmt
from taos.subscription import TaosSubscription
from taos import connection as connection_module
from taos import cursor as cursor_module
from taos import result as result_module
from taos import statement as statement_module
from taos import statement2
from taos import subscription as subscription_module


def _timestamp_buffer(value):
    return (ctypes.c_int64 * 1)(value)


def test_timestamp_precision_uses_reported_metadata():
    far_future_millis = 100000000000000
    result = field._crow_timestamp_to_python(
        _timestamp_buffer(far_future_millis), [False], 1, precision=FieldType.C_TIMESTAMP_MILLI, timezone=None
    )
    assert result == [field._datetime_epoch + timedelta(milliseconds=far_future_millis)]

    result = field._crow_timestamp_to_python(
        _timestamp_buffer(1), [False], 1, precision=FieldType.C_TIMESTAMP_MICRO, timezone=None
    )
    assert result == [field._datetime_epoch + timedelta(microseconds=1)]


def test_explicit_timezone_is_independent_from_legacy_global_state():
    naive = datetime(2020, 1, 1, 0, 0, 0)
    utc = pytz.UTC
    shanghai = pytz.timezone("Asia/Shanghai")
    try:
        field.set_tz(pytz.timezone("America/Los_Angeles"))
        utc_value = datetime_to_timestamp(naive, PrecisionEnum.Milliseconds, tz=utc).value
        shanghai_value = datetime_to_timestamp(naive, PrecisionEnum.Milliseconds, tz=shanghai).value
        assert utc_value - shanghai_value == 8 * 60 * 60 * 1000
        assert datetime_to_timestamp(utc.localize(naive), PrecisionEnum.Milliseconds, tz=shanghai).value == utc_value

        raw = _timestamp_buffer(0)
        assert field._crow_timestamp_to_python(
            raw, [False], 1, precision=FieldType.C_TIMESTAMP_MILLI, timezone=utc
        ) == [utc.localize(datetime(1970, 1, 1))]
        assert field._crow_timestamp_to_python(
            raw, [False], 1, precision=FieldType.C_TIMESTAMP_MILLI, timezone=shanghai
        ) == [shanghai.localize(datetime(1970, 1, 1, 8, 0, 0))]
    finally:
        field.set_tz(None)


def test_stmt2_bind_uses_statement_timezone():
    stmt = SimpleNamespace(
        fields=[SimpleNamespace(type=FieldType.C_TIMESTAMP, precision=PrecisionEnum.Milliseconds)], _timezone=pytz.UTC
    )
    bind = statement2.createColsBind(stmt, [[[datetime(2020, 1, 1, 0, 0, 0)]]])[0][0]
    value = ctypes.cast(bind.buffer, ctypes.POINTER(ctypes.c_int64))[0]
    assert value == 1577836800000

    direct_bind = bind2.TaosStmt2Bind()
    direct_bind.set_value(
        FieldType.C_TIMESTAMP,
        [datetime(2020, 1, 1, 0, 0, 0)],
        PrecisionEnum.Milliseconds,
        pytz.timezone("Asia/Shanghai"),
    )
    value = ctypes.cast(direct_bind.buffer, ctypes.POINTER(ctypes.c_int64))[0]
    assert value == 1577808000000


def test_fetch_result_fields_falls_back_when_extended_api_is_missing(monkeypatch):
    result = object()
    monkeypatch.setattr(cinterface, "IS_V3", True)
    monkeypatch.setitem(cinterface._UNSUPPORTED, "taos_fetch_fields_e", AttributeError("missing symbol"))
    monkeypatch.setattr(cinterface, "taos_fetch_fields", lambda value: ("legacy", value))
    monkeypatch.setattr(cinterface, "taos_fetch_fields_e", lambda value: (_ for _ in ()).throw(AssertionError()))
    assert cinterface.taos_fetch_result_fields(result) == ("legacy", result)


def test_only_timezone_state_statements_are_recognized():
    assert TaosConnection._is_set_timezone_statement("SET TIMEZONE 'Asia/Shanghai'")
    assert TaosConnection._is_set_timezone_statement("/* connection setup */ SET TIMEZONE 'UTC'")
    assert TaosConnection._is_set_timezone_statement('ALTER LOCAL "timezone" "UTC"')
    assert not TaosConnection._is_set_timezone_statement("ALTER LOCAL 'timezone Asia/Shanghai'")
    assert not TaosConnection._is_set_timezone_statement("SELECT 'SET TIMEZONE UTC'")
    assert not TaosConnection._is_set_timezone_statement("SET timezone_name 'UTC'")


@pytest.mark.parametrize(
    "timezone_value, expected_offset",
    [
        ("+08:00 (UTC, +0800)", timedelta(hours=8)),
        ("+0800", timedelta(hours=8)),
        ("-05:30 (UTC, -0530)", timedelta(hours=-5, minutes=-30)),
        ("UTC-8 (UTC-8, +0800)", timedelta(hours=8)),
        ("UTC-08:30", timedelta(hours=8, minutes=30)),
        ("UTC+5:30 (UTC+5:30, -0530)", timedelta(hours=-5, minutes=-30)),
        ("UTC+0 (UTC, +0000)", timedelta(0)),
    ],
)
def test_connection_timezone_parser_supports_fixed_offsets(timezone_value, expected_offset):
    timezone = TaosConnection._parse_python_timezone(timezone_value)
    assert timezone.utcoffset(None) == expected_offset


@pytest.mark.parametrize(
    "timezone_value, expected_name",
    [
        ("Asia/Shanghai (CST, +0800)", "Asia/Shanghai"),
        ("America/St_Johns (NST, -0330)", "America/St_Johns"),
        ("China Standard Time (CST, +0800)", "Asia/Shanghai"),
        ("Pacific Standard Time (PST, -0800)", "America/Los_Angeles"),
        ("Central Standard Time (Mexico)", "America/Mexico_City"),
    ],
)
def test_connection_timezone_parser_supports_iana_and_windows_names(timezone_value, expected_name):
    assert TaosConnection._parse_python_timezone(timezone_value).zone == expected_name


@pytest.mark.parametrize("timezone_value", ["+24:00", "+08:60", "UTC-24", "UTC+08:60", "Unknown Time Zone"])
def test_connection_timezone_parser_rejects_invalid_values(timezone_value):
    with pytest.raises(pytz.UnknownTimeZoneError):
        TaosConnection._parse_python_timezone(timezone_value)


def test_connection_timezone_parser_accepts_bytes_from_timezone_query():
    connection = object.__new__(TaosConnection)
    connection._conn = None
    connection._set_python_timezone(b"China Standard Time (CST, +0800)")
    assert connection._timezone.zone == "Asia/Shanghai"


@pytest.mark.parametrize(
    "timezone_value, expected_offset",
    [
        ("UTC (UTC, +0000)", timedelta(0)),
        ("+05:45 (UTC, +0545)", timedelta(hours=5, minutes=45)),
        ("UTC+3:30 (UTC+3:30, -0330)", timedelta(hours=-3, minutes=-30)),
    ],
)
def test_connection_timezone_query_values_update_connection_state(timezone_value, expected_offset):
    connection = object.__new__(TaosConnection)
    connection._conn = None
    connection._set_python_timezone(timezone_value)
    assert connection._timezone.utcoffset(None) == expected_offset


def test_connection_timezone_parser_supports_all_windows_timezone_names():
    for windows_name, iana_name in win_tz.items():
        assert TaosConnection._parse_python_timezone(windows_name).zone == iana_name


def test_connection_syncs_only_after_successful_set_timezone(monkeypatch):
    class FakeResult:
        def __init__(self, result, **kwargs):
            self.result = result
            self.kwargs = kwargs

    connection = object.__new__(TaosConnection)
    connection._conn = ctypes.c_void_p(1)
    connection._timezone = None
    connection.decode_binary = True
    calls = []
    native_close_calls = []

    monkeypatch.setattr(connection_module, "taos_query", lambda _conn, _sql: ctypes.c_void_p(1))
    monkeypatch.setattr(connection_module, "taos_errno", lambda _result: 0)
    monkeypatch.setattr(connection_module, "taos_close", lambda _conn: native_close_calls.append(_conn))
    monkeypatch.setattr(connection_module, "TaosResult", FakeResult)
    monkeypatch.setattr(connection, "_sync_python_timezone", lambda: calls.append("sync"))

    try:
        connection.query("ALTER LOCAL 'timezone Asia/Shanghai'")
        assert calls == []
        connection.query("SET TIMEZONE 'Asia/Shanghai'")
        assert calls == ["sync"]
        connection.query('ALTER LOCAL "timezone" "UTC"')
        assert calls == ["sync", "sync"]
    finally:
        # This object bypasses __init__ and owns no native connection handle.
        connection._conn = None

    connection.__del__()
    assert native_close_calls == []


def test_cursor_query_does_not_write_diagnostic_output(monkeypatch, capsys):
    connection = SimpleNamespace(
        _conn=ctypes.c_void_p(1),
        _timezone=pytz.UTC,
        _is_set_timezone_statement=TaosConnection._is_set_timezone_statement,
    )
    cursor = TaosCursor(connection)

    monkeypatch.setattr(cursor_module, "taos_query", lambda _conn, _sql: object())
    monkeypatch.setattr(cursor_module, "taos_field_count", lambda _result: 1)
    monkeypatch.setattr(cursor_module, "taos_fetch_result_fields", lambda _result: [])
    monkeypatch.setattr(cursor, "_handle_result", lambda: "result")

    assert cursor._execute_sql("SELECT greatest(1, 2)") == "result"
    assert capsys.readouterr().out == ""
    cursor._result = None


def test_result_forwards_its_timezone_to_block_conversion(monkeypatch):
    observed = {}

    def fake_fetch_block(_result, **kwargs):
        observed.update(kwargs)
        return None, 0

    monkeypatch.setattr(result_module, "taos_fetch_block", fake_fetch_block)
    result = result_module.TaosResult(ctypes.c_void_p(1), timezone=pytz.UTC)
    try:
        result.fetch_block()
    except StopIteration:
        pass
    else:
        raise AssertionError("fetch_block should stop at an empty result")
    assert observed["timezone"] is pytz.UTC


def test_statement_and_subscription_preserve_their_timezone(monkeypatch):
    observed = {}

    class FakeResult:
        def __init__(self, result, **kwargs):
            observed.update(result=result, **kwargs)

    monkeypatch.setattr(statement_module, "taos_stmt_use_result", lambda _stmt: ctypes.c_void_p(2))
    monkeypatch.setattr(statement_module, "taos_stmt_close", lambda _stmt: None)
    monkeypatch.setattr(statement_module, "TaosResult", FakeResult)
    stmt = TaosStmt(ctypes.c_void_p(1), decode_binary=False, timezone=pytz.UTC)
    stmt.use_result()
    assert observed["result"].value == 2
    assert observed["close_after"] is False
    assert observed["decode_binary"] is False
    assert observed["timezone"] is pytz.UTC
    stmt.close()

    observed.clear()
    monkeypatch.setattr(subscription_module, "taos_consume", lambda _sub: ctypes.c_void_p(3))
    monkeypatch.setattr(subscription_module, "taos_unsubscribe", lambda _sub, _keep_progress: None)
    monkeypatch.setattr(subscription_module, "TaosResult", FakeResult)
    subscription = TaosSubscription(ctypes.c_void_p(1), timezone=pytz.UTC)
    subscription.consume()
    assert observed["result"].value == 3
    assert observed["timezone"] is pytz.UTC
    assert subscription.close()


def test_subscription_default_timezone_keeps_legacy_behavior():
    subscription = TaosSubscription(None)
    assert subscription._timezone is DEFAULT_TIMEZONE
