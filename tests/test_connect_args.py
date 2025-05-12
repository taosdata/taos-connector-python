from taos import *


def test_connect_args():
    """
    DO NOT DELETE THIS TEST CASE!

    Useless args, prevent mistakenly deleted args in connect init.
    Because some case in CI of earlier version may use it.
    """
    host = "192.168.2.140"
    conn = connect(host)
    conn.options_connection(0, "utf8")
    conn.options_connection(1, "UTC")
    conn.options_connection(2, "127.0.0.2")
    conn.options_connection(3, "python client")
    conn.set_conn_mode(0, 1)
    
    result = conn.query("select last(*) from test.meters")
    data = result.fetch_all()
    print(data)
    assert data is None
    # assert rows is not None
    # print(rows)
    # for row in result:
    #     print(row)
    assert conn is not None
    conn.close()
