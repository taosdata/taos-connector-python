import datetime

from taosrest.restclient import RestClient
import taos


def test_auth():
    if taos.IS_V3:
        return
    client = RestClient("localhost:6041", user="root", password="taosdata")
    print(client._taosd_token)


def test_show_database():
    if taos.IS_V3:
        return
    client = RestClient("localhost:6041")
    resp = client.sql("show databases")
    print("\n", resp)
    # {'status': 'succ', 'head': ['name', 'created_time', 'ntables', 'vgroups', 'replica', 'quorum', 'days', 'keep', 'cache(MB)', 'blocks', 'minrows', 'maxrows', 'wallevel', 'fsync', 'comp', 'cachelast', 'precision', 'update', 'status'], 'column_meta': [['name', 8, 32], ['created_time', 9, 8], ['ntables', 4, 4], ['vgroups', 4, 4], ['replica', 3, 2], ['quorum', 3, 2], ['days', 3, 2], ['keep', 8, 24], ['cache(MB)', 4, 4], ['blocks', 4, 4], ['minrows', 4, 4], ['maxrows', 4, 4], ['wallevel', 2, 1], ['fsync', 4, 4], ['comp', 2, 1], ['cachelast', 2, 1], ['precision', 8, 3], ['update', 2, 1], ['status', 8, 10]], 'data': [['test', '2022-04-07T13:15:26.857+08:00', 8, 1, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready'], ['log', '2022-03-26T15:54:26.997+08:00', 150, 1, 1, 1, 10, '30', 1, 3, 100, 4096, 1, 3000, 2, 0, 'us', 0, 'ready'], ['power', '2022-04-14T14:44:00.059+08:00', 4, 1, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready']], 'rows': 3}


def test_insert_data():
    if taos.IS_V3:
        return
    c = RestClient("localhost:6041", password="taosdata")
    c.sql("drop database if exists test")
    c.sql("create database test")
    resp = c.sql("create table test.tb2 (ts timestamp, c1 int, c2 double, c3 timestamp)")
    print("\n=====================create table resp================")
    print(resp)
    # {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[0]]}
    resp = c.sql("insert into test.tb2 values (now, -100, -200.3, now+1m) (now+10s, -101, -340.2423424, now+2m)")
    print("==============insert resp==============")
    print(resp)
    #  {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[2]]}
    assert resp["rows"] == 1
    assert resp["head"] == ['affected_rows']


def test_describe_table():
    if taos.IS_V3:
        return
    c = RestClient("localhost:6041")
    try:
        c.sql("describe test.noexits")
        assert False
    except:
        pass


def test_select_data_with_timestamp_type():
    if taos.IS_V3:
        return
    c = RestClient("localhost:6041")
    resp = c.sql("select * from test.tb2")
    print("\n", resp)
    data = resp["data"]
    assert isinstance(data[0][0], datetime.datetime)
    assert isinstance(data[0][3], datetime.datetime)
    #  {'status': 'succ', 'head': ['ts', 'c1', 'c2', 'c3'], 'column_meta': [['ts', 9, 8], ['c1', 4, 4], ['c2', 7, 8], ['c3', 9, 8]], 'data': [[datetime.datetime(2022, 4, 21, 9, 14, 50, 498000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), -100, -200.3, datetime.datetime(2022, 4, 21, 9, 15, 50, 498000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800)))], [datetime.datetime(2022, 4, 21, 9, 15, 0, 498000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), -101, -340.2423424, datetime.datetime(2022, 4, 21, 9, 16, 50, 498000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800)))]], 'rows': 2}
