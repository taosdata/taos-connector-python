from taos import *
import time

def test_connect_args():
    """
    DO NOT DELETE THIS TEST CASE!

    Useless args, prevent mistakenly deleted args in connect init.
    Because some case in CI of earlier version may use it.
    """
 
    conn = connect()
    conn.options_connection(0, "utf8")
    conn.options_connection(1, "UTC")
    conn.options_connection(2, "127.0.0.2")
    conn.options_connection(3, "python client")
    conn.set_conn_mode(0, 1)
    
    time.sleep(10)
    result = conn.query("show connections")
    assert result is not None
    for row in result:
        assert row[7] == "python client"
        assert row[8] == "127.0.0.2"

    result = conn.query("select timezone()")
    assert result is not None
    for row in result:
        assert row[0] == "UTC (UTC, +0000)"
    
    assert conn is not None
    conn.close()
