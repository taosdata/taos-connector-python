import pandas
import taosrest
import taos
from sqlalchemy import create_engine
from datetime import datetime


def test_insert_test_data():
    conn = taos.connect()
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")


def test_pandas_read_from_rest_connection():
    if taos.IS_V3:
        return
    conn = taosrest.connect()
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_native_connection():
    conn = taos.connect()
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_sqlalchemy_taosrest():
    if taos.IS_V3:
        return
    engine = create_engine("taosrest://root:taosdata@localhost:6041")
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
