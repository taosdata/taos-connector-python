import ast
import importlib
import os
from pathlib import Path
from urllib.parse import urlparse

import pytest
import taosws
import utils

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import make_url


TDENGINE_URL = os.getenv("TDENGINE_URL")
requires_tdengine = pytest.mark.skipif(TDENGINE_URL is None, reason="Please set environment variable TDENGINE_URL")
registry.register("taosws", "taosws.sqlalchemy", "TaosWsDialect")


def resolve_tdengine_host_port(url):
    normalized = url if "://" in url else f"ws://{url}"
    parsed = urlparse(normalized)
    return parsed.hostname or "localhost", parsed.port or 6041


HOST, PORT = resolve_tdengine_host_port(TDENGINE_URL) if TDENGINE_URL else ("localhost", 6041)


def insert_data(conn=None):
    close_on_exit = conn is None
    c = conn or taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}")
    c.execute("drop database if exists test")
    c.execute("create database if not exists test")
    c.execute("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)")
    c.execute("insert into test.d0 using test.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)")
    c.execute("insert into test.d1 using test.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)")
    c.execute("create table test.ntb(ts timestamp, age int)")
    c.execute("insert into test.ntb values(now, 23)")
    if close_on_exit:
        c.close()


def check_basic(conn, inspection, sub_tables=None):
    tables = sub_tables or ["meters", "ntb"]

    databases = inspection.get_schema_names()
    assert "test" in databases, f"test not in {databases}"

    assert inspection.get_table_names("test") == tables, "check get_table_names() failed"

    expected_columns = [
        {"name": "ts", "type": inspection.dialect._resolve_type("TIMESTAMP")},
        {"name": "c1", "type": inspection.dialect._resolve_type("INT")},
        {"name": "c2", "type": inspection.dialect._resolve_type("DOUBLE")},
        {"name": "t1", "type": inspection.dialect._resolve_type("INT")},
    ]
    columns = inspection.get_columns("meters", "test")
    for index, column in enumerate(columns):
        expected = expected_columns[index]
        assert column["name"] == expected["name"], f"column name mismatch: {column['name']} != {expected['name']}"
        assert (
            type(column["type"]) == expected["type"]
        ), f"column type mismatch: {type(column['type'])} != {expected['type']}"

    assert inspection.has_table("meters", "test") is True, "check has_table() failed"
    assert inspection.dialect.has_schema(conn, "test") is True, "check has_schema() failed"

    conn.close()


@requires_tdengine
def test_read_from_sqlalchemy_taosws():
    engine = create_engine(
        f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_data()
    inspection = inspect(engine)
    check_basic(conn, inspection)


@requires_tdengine
def test_read_from_sqlalchemy_taosws_failover():
    db_name = "test_1755496227"
    conn = taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}")
    conn.execute(f"drop database if exists {db_name}")
    conn.execute(f"create database {db_name}")

    try:
        urls = [
            "taosws://",
            f"taosws://{HOST}",
            f"taosws://{HOST}:{PORT}",
            f"taosws://{HOST}:{PORT}/{db_name}",
            f"taosws://root@{HOST}:{PORT}/{db_name}",
            f"taosws://root:@{HOST}:{PORT}/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}/{db_name}?hosts=",
            f"taosws://{utils.test_username()}:{utils.test_password()}@/{db_name}?hosts={HOST}:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}/{db_name}?hosts={HOST}:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}/{db_name}?hosts={HOST}:{PORT},127.0.0.1:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}/{db_name}?hosts={HOST}:{PORT},127.0.0.1:{PORT}&timezone=Asia/Shanghai",
        ]

        for url in urls:
            engine = create_engine(url)
            econn = engine.connect()
            econn.close()

        invalid_urls = [
            f"taosws://:{PORT}",
            f"taosws://:taosdata@=localhost:{PORT}/{db_name}",
        ]

        for url in invalid_urls:
            with pytest.raises(Exception):
                engine = create_engine(url)
                econn = engine.connect()
                econn.close()

    finally:
        conn.execute(f"drop database if exists {db_name}")
        conn.close()


def test_taosws_sqlalchemy_module_is_available():
    module = importlib.import_module("taosws.sqlalchemy")
    assert hasattr(module, "TaosWsDialect")


def test_resolve_type_covers_all_declared_tdengine_types():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()

    for tdengine_type, sqlalchemy_type in module.TYPES_MAP.items():
        assert dialect._resolve_type(tdengine_type) is sqlalchemy_type

    assert dialect._resolve_type("TYPE_NOT_EXISTS") is sqltypes.UserDefinedType


def test_create_connect_args_prefers_hosts_and_keeps_other_query_params():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url(
        "taosws://root:taosdata@localhost:6041/test_1774703606?"
        "hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041,127.0.0.1:6041/test_1774703606?timezone=Asia%2FShanghai"]
    assert kwargs == {}


def test_create_connect_args_no_trailing_ampersand_when_hosts_is_last_param():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url(
        "taosws://root:taosdata@localhost:6041/test_1774703606?" "timezone=Asia/Shanghai&hosts=localhost:6041"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041/test_1774703606?timezone=Asia%2FShanghai"]
    assert kwargs == {}


def test_create_connect_args_encodes_query_values_safely():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url("taosws://root:taosdata@localhost:6041/test_1774703606?note=cn north")

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041/test_1774703606?note=cn%20north"]
    assert kwargs == {}


def test_create_connect_args_normalizes_explicit_empty_password():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url("taosws://root:@localhost:6041/test_1774703606")

    args, kwargs = dialect.create_connect_args(url)

    # taosws rejects explicit empty password (root:@...), so we normalize it.
    assert args == ["taosws://root@localhost:6041/test_1774703606"]
    assert kwargs == {}


def test_sqlalchemy_module_does_not_depend_on_taospy_imports():
    module = importlib.import_module("taosws.sqlalchemy")
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                top_level_name = alias.name.split(".")[0]
                assert top_level_name not in {"taos", "taospy"}
