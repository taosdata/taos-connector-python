import taosws
import time
import os


def test_ws_connect():
    print("-" * 40)
    print("test_ws_connect")
    conn = taosws.connect("taosws://root:taosdata@localhost:6041")
    r = conn.query_with_req_id("show dnodes", 1)
    print("r: ", r.fields)
    print("test_ws_connect done")
    print("-" * 40)


def test_default_connect():
    print("-" * 40)
    print("test_default_connect")
    conn = taosws.connect()
    r = conn.query_with_req_id("show dnodes", 1)
    print("r: ", r.fields)
    print("test_default_connect done")
    print("-" * 40)


def test_connect_invalid_user():
    print("-" * 40)
    print("test_connect_invalid_user")
    try:
        conn = taosws.connect(
            user="taos",
            password="taosdata",
            host="localhost",
            port=6041,
        )
        r = conn.query_with_req_id("show dnodes", 1)
        print("r: ", r.fields)
    except Exception as e:
        print("except invalid_user: ", e)
    print("test_connect_invalid_user done")
    print("-" * 40)


def test_report_connector_info():
    test = os.getenv("TEST_TD_3360")
    if test is not None:
        return

    conn1 = taosws.connect()
    time.sleep(2)
    res = conn1.query("show connections")
    found = False
    for row in res:
        connector_info = row[row.__len__() - 1]
        if "python-ws" in str(connector_info):
            found = True
            print("connector_info:", connector_info)
    assert found

    conn2 = taosws.connect(
        user="root",
        password="taosdata",
        host="localhost",
        port=6041,
    )
    time.sleep(2)
    res = conn2.query("show connections")
    found = False
    for row in res:
        connector_info = row[row.__len__() - 1]
        if "python-ws" in str(connector_info):
            found = True
            print("connector_info:", connector_info)
    assert found

    conn1.close()
    conn2.close()


def show_env():
    import os

    print("-" * 40)
    print("TAOS_LIBRARY_PATH: ", os.environ.get("TAOS_LIBRARY_PATH"))
    print("TAOS_CONFIG_DIR: ", os.environ.get("TAOS_CONFIG_DIR"))
    print("TAOS_C_CLIENT_VERSION: ", os.environ.get("TAOS_C_CLIENT_VERSION"))
    print("TAOS_C_CLIENT_VERSION_STR: ", os.environ.get("TAOS_C_CLIENT_VERSION_STR"))
    print("taosws.__version__", taosws.__version__)
    print("-" * 40)


if __name__ == "__main__":
    show_env()
    test_ws_connect()
    test_default_connect()
    test_connect_invalid_user()
    test_report_connector_info()
