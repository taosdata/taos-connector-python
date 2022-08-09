import os

import pandas
import taosrest
import taos
import taosws
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from decorators import check_env

load_dotenv()


def test_insert_test_data():
    conn = taos.connect()
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")


@check_env
def test_pandas_read_from_rest_connection():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url)
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_native_connection():
    conn = taos.connect()
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


@check_env
def test_pandas_read_from_sqlalchemy_taosrest():
    url = os.environ["SQLALCHEMY_URL"]  # "taosrest://root:taosdata@vm95:6061"
    engine = create_engine(url)
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", engine)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_sqlalchemy_taos():
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", engine)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


if __name__ == '__main__':
    test_pandas_read_from_sqlalchemy_taosrest()
