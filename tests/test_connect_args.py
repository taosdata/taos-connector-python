from taos import *


def test_connect_args():
    """
    DO NOT DELETE THIS TEST CASE!

    Useless args, prevent mistakenly deleted args in connect init.
    Because some case in CI of earlier version may use it.
    """
    host = 'localhost:6030'
    conn = connect(host)
    assert conn is not None
    conn.close()
