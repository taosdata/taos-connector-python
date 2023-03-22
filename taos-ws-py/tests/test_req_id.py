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