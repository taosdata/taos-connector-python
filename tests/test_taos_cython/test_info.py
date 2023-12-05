from taos._objects import TaosConnection
import pytest


@pytest.fixture
def conn():
    return TaosConnection(host="localhost")


def test_client_info(conn):
    print("client info: %s" % conn.client_info)
    pass


def test_server_info(conn):
    # type: (TaosConnection) -> None
    print("conn client info: %s" % conn.client_info)
    print("conn server info: %s" % conn.server_info)
    pass
