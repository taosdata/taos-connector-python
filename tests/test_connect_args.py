from taos import *


def test_connect_args():
    host = 'localhost:6030'
    conn = connect(host)
    assert conn is not None
    conn.close()
