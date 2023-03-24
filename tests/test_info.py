from taos.cinterface import *

from taos import *

import pytest


@pytest.fixture
def conn():
    return connect()


def test_client_info():
    print("client info: %s" % taos_get_client_info())
    pass


def test_server_info(conn):
    # type: (TaosConnection) -> None
    print("conn client info: %s" % conn.client_info)
    print("conn server info: %s" % conn.server_info)
    pass


if __name__ == "__main__":
    test_client_info()
    test_server_info(connect())
