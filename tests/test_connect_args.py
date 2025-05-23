from taos import *
import time

from taos.constants import TSDB_CONNECTIONS_MODE, TSDB_OPTION_CONNECTION

def test_connect_args():
    """
    DO NOT DELETE THIS TEST CASE!

    Useless args, prevent mistakenly deleted args in connect init.
    Because some case in CI of earlier version may use it.
    """
 
    host = "localhost:6030"
    conn = connect(host=host, charset="utf8", timezone="UTC", user_app="python client 1", user_ip="127.2.2.2", bi_mode=True)
   
    time.sleep(30)
    bClient = False
    bHost = False
    result = conn.query("show connections")
    assert result is not None
    for row in result:
        print(row)
        if row[7] == "python client 1":
            bClient = True
        if row[8] == "127.2.2.2":
            bHost = True

    assert bClient and bHost
    
    result = conn.query("select timezone()")
    assert result is not None
    for row in result:
        print(row)
        assert row[0] == "UTC (UTC, +0000)"

    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_CHARSET.value, "utf8")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_TIMEZONE.value, "UTC")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP.value, "127.0.0.2")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP.value, "python client")
    conn.set_mode(TSDB_CONNECTIONS_MODE.TSDB_CONNECTIONS_MODE_BI.value, 1)
    
    time.sleep(30)
    bClient = False
    bHost = False
    result = conn.query("show connections")
    assert result is not None
    for row in result:
        print(row)
        if row[7] == "python client":
            bClient = True
        if row[8] == "127.0.0.2":
            bHost = True

    assert bClient and bHost
    
    result = conn.query("select timezone()")
    assert result is not None
    for row in result:
        print(row)
        assert row[0] == "UTC (UTC, +0000)"
    
    assert conn is not None
    conn.close()
