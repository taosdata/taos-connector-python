import os
from urllib.parse import urlparse

import pytest
import taosws
import utils

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.dialects import registry


TDENGINE_URL = os.getenv("TDENGINE_URL")
pytestmark = pytest.mark.skipif(TDENGINE_URL is None, reason="Please set environment variable TDENGINE_URL")
registry.register("taosws", "taosws.sqlalchemy", "TaosWsDialect")


def resolve_tdengine_host_port(url):
    normalized = url if "://" in url else f"ws://{url}"
    parsed = urlparse(normalized)
    return parsed.hostname or "localhost", parsed.port or 6041


HOST, PORT = resolve_tdengine_host_port(TDENGINE_URL) if TDENGINE_URL else ("localhost", 6041)

SQLALCHEMY_DB = "test_1774703601"
FAILOVER_DB = "test_1774703602"
EXECUTE_DB = "test_1774703603"
INDEX_INJECTION_DB_1 = "test_1774703604"
INDEX_INJECTION_DB_2 = "test_1774703605"


def insert_data(conn=None, db_name=SQLALCHEMY_DB):
    close_on_exit = conn is None
    c = conn or taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}")
    c.execute(f"drop database if exists {db_name}")
    c.execute(f"create database if not exists {db_name}")
    c.execute(f"create table {db_name}.meters (ts timestamp, c1 int, c2 double) tags(t1 int)")
    c.execute(
        f"insert into {db_name}.d0 using {db_name}.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)"
    )
    c.execute(
        f"insert into {db_name}.d1 using {db_name}.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)"
    )
    c.execute(f"create table {db_name}.ntb(ts timestamp, age int)")
    c.execute(f"insert into {db_name}.ntb values(now, 23)")
    if close_on_exit:
        c.close()


def check_basic(conn, inspection, schema=SQLALCHEMY_DB, sub_tables=None):
    tables = sub_tables or ["meters", "ntb"]

    databases = inspection.get_schema_names()
    assert schema in databases, f"{schema} not in {databases}"

    assert inspection.get_table_names(schema) == tables, "check get_table_names() failed"

    expected_columns = [
        {"name": "ts", "type": inspection.dialect._resolve_type("TIMESTAMP")},
        {"name": "c1", "type": inspection.dialect._resolve_type("INT")},
        {"name": "c2", "type": inspection.dialect._resolve_type("DOUBLE")},
        {"name": "t1", "type": inspection.dialect._resolve_type("INT")},
    ]
    columns = inspection.get_columns("meters", schema)
    for index, column in enumerate(columns):
        expected = expected_columns[index]
        assert column["name"] == expected["name"], f"column name mismatch: {column['name']} != {expected['name']}"
        assert (
            type(column["type"]) == expected["type"]
        ), f"column type mismatch: {type(column['type'])} != {expected['type']}"

    assert inspection.has_table("meters", schema) is True, "check has_table() failed"
    assert inspection.dialect.has_schema(conn, schema) is True, "check has_schema() failed"

    conn.close()


def test_read_from_sqlalchemy_taosws():
    engine = create_engine(
        f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_data(db_name=SQLALCHEMY_DB)
    inspection = inspect(engine)
    check_basic(conn, inspection, schema=SQLALCHEMY_DB)


def test_read_from_sqlalchemy_taosws_failover():
    db_name = FAILOVER_DB
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


def test_sqlalchemy_taosws_execute_and_executemany_params():
    db_name = EXECUTE_DB
    engine = create_engine(
        f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    try:
        conn.execute(text(f"drop database if exists {db_name}"))
        conn.execute(text(f"create database if not exists {db_name}"))
        conn.execute(text(f"create table {db_name}.ntb(ts timestamp, v int)"))

        conn.execute(text(f"insert into {db_name}.ntb values (:ts, :v)"), {"ts": 1733189403001, "v": 1})
        conn.execute(
            text(f"insert into {db_name}.ntb values (:ts, :v)"),
            [
                {"ts": 1733189403002, "v": 2},
                {"ts": 1733189403003, "v": 3},
            ],
        )

        rows = conn.execute(text(f"select count(*) from {db_name}.ntb")).fetchall()
        assert rows[0][0] == 3
    finally:
        conn.execute(text(f"drop database if exists {db_name}"))
        conn.close()


def test_taosws_get_indexes_prevents_schema_injection():
    db_name_1, db_name_2 = INDEX_INJECTION_DB_1, INDEX_INJECTION_DB_2
    engine = create_engine(
        f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    try:
        conn.execute(text(f"drop database if exists {db_name_1}"))
        conn.execute(text(f"drop database if exists {db_name_2}"))
        conn.execute(text(f"create database if not exists {db_name_1}"))
        conn.execute(text(f"create database if not exists {db_name_2}"))

        conn.execute(
            text(f"create table {db_name_1}.meters(ts timestamp, c1 int, c2 double) tags(t1 int, location nchar(16))")
        )
        conn.execute(
            text(f"create table {db_name_2}.meters(ts timestamp, c1 int, c2 double) tags(t1 int, location nchar(16))")
        )
        conn.execute(text(f"create index idx_location_wa on {db_name_1}.meters(location)"))
        conn.execute(text(f"create index idx_location_wb on {db_name_2}.meters(location)"))

        inspection = inspect(engine)
        indexes = inspection.get_indexes("meters", db_name_1)
        assert any(idx.get("name") == "idx_location_wa" for idx in indexes)
        assert all(idx.get("name") != "idx_location_wb" for idx in indexes)

        injected_schema = f"{db_name_1}' OR '1'='1"
        injected_indexes = inspection.get_indexes("meters", injected_schema)
        assert injected_indexes == []
    finally:
        conn.execute(text(f"drop database if exists {db_name_1}"))
        conn.execute(text(f"drop database if exists {db_name_2}"))
        conn.close()
