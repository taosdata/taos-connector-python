import os

import pytest
import taosws
import utils
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.dialects import registry


pytest.importorskip("sqlalchemy")
pytestmark = pytest.mark.skipif(
    "TDENGINE_URL" not in os.environ, reason="Please set environment variable TDENGINE_URL"
)
registry.register("taosws", "taosws.sqlalchemy", "TaosWsDialect")

HOST = "localhost"
PORT = 6041


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


def check_list_equal(list1, list2, tips):
    if list1 != list2:
        raise BaseException(f"{tips} failed. list1={list1} list2={list2}")


def check_result_equal(result1, result2, tips):
    if result1 != result2:
        raise BaseException(f"{tips} failed. result1={result1} result2={result2}")


def check_basic(conn, inspection, sub_tables=None):
    tables = sub_tables or ["meters", "ntb"]

    databases = inspection.get_schema_names()
    if "test" not in databases:
        raise BaseException(f"test not in {databases}")

    check_list_equal(inspection.get_table_names("test"), tables, "check get_table_names()")

    expected_columns = [
        {"name": "ts", "type": inspection.dialect._resolve_type("TIMESTAMP")},
        {"name": "c1", "type": inspection.dialect._resolve_type("INT")},
        {"name": "c2", "type": inspection.dialect._resolve_type("DOUBLE")},
        {"name": "t1", "type": inspection.dialect._resolve_type("INT")},
    ]
    columns = inspection.get_columns("meters", "test")
    for index, column in enumerate(columns):
        expected = expected_columns[index]
        if column["name"] != expected["name"]:
            raise BaseException(f"column name mismatch: {column['name']} != {expected['name']}")
        if type(column["type"]) != expected["type"]:
            raise BaseException(f"column type mismatch: {type(column['type'])} != {expected['type']}")

    check_result_equal(inspection.has_table("meters", "test"), True, "check has_table()")
    check_result_equal(inspection.dialect.has_schema(conn, "test"), True, "check has_schema()")

    conn.close()


def test_read_from_sqlalchemy_taosws():
    engine = create_engine(
        f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_data()
    inspection = inspect(engine)
    check_basic(conn, inspection)


def test_read_from_sqlalchemy_taosws_failover():
    db_name = "test_1755496227"
    conn = taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@{HOST}:{PORT}")
    conn.execute(f"drop database if exists {db_name}")
    conn.execute(f"create database {db_name}")

    try:
        urls = [
            "taosws://",
            "taosws://localhost",
            f"taosws://localhost:{PORT}",
            f"taosws://localhost:{PORT}/{db_name}",
            f"taosws://root@localhost:{PORT}/{db_name}",
            f"taosws://root:@localhost:{PORT}/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:{PORT}/{db_name}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:{PORT}/{db_name}?hosts=",
            f"taosws://{utils.test_username()}:{utils.test_password()}@/{db_name}?hosts=localhost:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:{PORT}/{db_name}?hosts=localhost:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:{PORT}/{db_name}?hosts=localhost:{PORT},127.0.0.1:{PORT}",
            f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:{PORT}/{db_name}?hosts=localhost:{PORT},127.0.0.1:{PORT}&timezone=Asia/Shanghai",
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
