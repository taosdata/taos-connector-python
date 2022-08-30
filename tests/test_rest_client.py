import datetime
import os
import taos

from dotenv import load_dotenv

from decorators import check_env
from taosrest.restclient import RestClient

load_dotenv()


@check_env
def test_auth():
    url = os.environ["TDENGINE_URL"]
    client = RestClient(url, user="root", password="taosdata")
    print(client._taosd_token)


@check_env
def test_show_database():
    url = os.environ["TDENGINE_URL"]
    client = RestClient(url)
    resp = client.sql("show databases")
    print("\n", resp)


@check_env
def test_insert_data():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, password="taosdata", database="test")
    c.sql("drop database if exists test")
    c.sql("create database test")
    resp = c.sql("create table tb2 (ts timestamp, c1 int, c2 double, c3 timestamp)")
    print("\n=====================create table resp================")
    print(resp)
    # {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[0]]}
    resp = c.sql("insert into tb2 values (now, -100, -200.3, now+1m) (now+10s, -101, -340.2423424, now+2m)")
    print("==============insert resp==============")
    print(resp)
    #  {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[2]]}
    assert resp["rows"] == 1
    if taos.IS_V3:
        assert resp["column_meta"] == [['affected_rows', "INT", 4]]
    else:
        assert resp["column_meta"] == [['affected_rows', 4, 4]]


@check_env
def test_describe_table():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    try:
        c.sql("describe test.noexits")
        assert False
    except Exception as e:
        print(e)


@check_env
def test_select_data_with_timestamp_type():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    resp = c.sql("select * from test.tb2")
    print("\n", resp)
    data = resp["data"]
    assert isinstance(data[0][0], datetime.datetime) and data[0][0].tzinfo is None
    assert isinstance(data[0][3], datetime.datetime) and data[0][3].tzinfo is None

@check_env
def test_use_str_timestamp():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, convert_timestamp=False)
    resp = c.sql("select * from test.tb2")
    data = resp["data"]
    print(data[0][0], data[0][3])
    assert isinstance(data[0][0], str) and isinstance(data[0][3], str)

if __name__ == "__main__":
    test_show_database()