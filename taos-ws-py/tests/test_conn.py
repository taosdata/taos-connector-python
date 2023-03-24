import taosws


def test_native_connect():
    conn = taosws.connect()
    conn.query_with_req_id('show dnodes', 1)


def test_ws_connect():
    conn = taosws.connect('taosws://root:taosdata@localhost:6041')
    conn.query_with_req_id('show dnodes', 1)


if __name__ == '__main__':
    test_ws_connect()
    test_native_connect()
