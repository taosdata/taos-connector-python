import datetime

import taosrest
import pytest
import os
from decorators import check_env
from dotenv import load_dotenv

load_dotenv()


@check_env
def test_fetch_all():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url,
                            password="taosdata")
    cursor = conn.cursor()

    cursor.execute("show databases")
    results: list[tuple] = cursor.fetchall()
    for row in results:
        print(row)
    print(cursor.description)


@check_env
def test_fetch_one():
    url = os.environ["TDENGINE_URL"]

    conn = taosrest.connect(url=url,
                            user="root",
                            password="taosdata")
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.executemany("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    assert c.rowcount == 2
    assert c.affected_rows == 2
    c.execute("select * from test.tb")
    assert c.rowcount == 2
    assert c.affected_rows is None
    print()
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()


@check_env
def test_row_count():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, user="root", password="taosdata")
    cursor = conn.cursor()
    cursor.execute("select * from test.tb")
    assert cursor.rowcount == 2


@check_env
def test_get_server_info():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url,
                            user="root",
                            password="taosdata")

    version: str = conn.server_info
    assert len(version.split(".")) == 4


@check_env
def test_execute():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    affected_rows = c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    assert affected_rows == 2
    affected_rows = c.execute("select * from test.tb")
    assert affected_rows is None


@check_env
def test_query():
    """
    Note: run it immediately after `test_execute`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb")
    assert r.rows == 2


@check_env
def test_default_database():
    """
    Note: run it immediately after `test_query`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, database="test")
    r = c.query("select * from tb")
    assert r.rows == 2


@check_env
def test_no_timezone():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb")
    for row in r:
        print(row)  # [datetime.datetime(2022, 7, 26, 5, 56, 58, 746000), -100, -200.3]


@check_env
def test_str_timezone():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, timezone="Asia/Shanghai")
    r = c.query("select * from test.tb")
    for row in r:
        print(row)  # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000, tzinfo=<DstTzInfo 'Asia/Shanghai' CST+8:00:00 STD>), -100, -200.3]


@check_env
def test_tzinfo_timezone():
    url = os.environ["TDENGINE_URL"]
    tz = datetime.datetime.now().astimezone().tzinfo
    c = taosrest.connect(url=url, timezone=tz)
    r = c.query("select * from test.tb")
    for row in r:
        print(row)  # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), 'CST')), -100, -200.3]
