import taosws


def test_query():
    ws = taosws.connect("taosws://root:taosdata@localhost:6041")
    res = ws.query_with_req_id('show dnodes', 1)
    print(f'res: {res}')


def test_execute():
    ws = taosws.connect("taosws://root:taosdata@localhost:6041")
    res = ws.execute_with_req_id('show dnodes', 1)
    print(f'res: {res}')


def test_cursor_execute():
    ws = taosws.connect("taosws://root:taosdata@localhost:6041")
    cur = ws.cursor()
    res = cur.execute_with_req_id('show dnodes', 1)
    print(f'res: {res}')


def test_cursor_execute_many():
    ws = taosws.connect("taosws://root:taosdata@localhost:6041")
    cur = ws.cursor()
    db = "t_ws"
    cur.execute("drop database if exists {}", db)
    cur.execute("create database {}", db)
    cur.execute("use {name}", name=db)
    cur.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    data = [{"name": "tb1", "t1": 1}, {"name": "tb2", "t1": 2}]
    res = cur.execute_many_with_req_id("create table {name} using stb tags({t1})", data, 1)
    print(f'res: {res}')
