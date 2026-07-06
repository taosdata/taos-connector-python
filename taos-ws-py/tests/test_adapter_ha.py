import time
import utils
import pytest
import taosws
from taosws import Consumer


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_connect_with_adapter_ha_dsn():
    conn = taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041?adapter_ha=true")
    try:
        assert_server_version(conn)
    finally:
        conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_connect_with_adapter_ha_kwargs():
    conn = taosws.connect(
        host="localhost",
        port=6041,
        user=utils.test_username(),
        password=utils.test_password(),
        adapter_ha="true",
    )
    try:
        assert_server_version(conn)
    finally:
        conn.close()


def assert_server_version(conn):
    rows = list(conn.query("select server_version()"))
    assert len(rows) == 1
    assert rows[0][0]


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_consumer_with_adapter_ha_dsn():
    init_topic()
    try:
        consumer = Consumer(
            dsn=f"ws://{utils.test_username()}:{utils.test_password()}@localhost:6041?&group.id=3731&auto.offset.reset=earliest&adapter_ha=true"
        )
        assert_consumer_can_poll(consumer)
    finally:
        cleanup_topic()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_consumer_with_adapter_ha_conf():
    init_topic()
    try:
        consumer = Consumer(
            {
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": "6041",
                "td.connect.user": utils.test_username(),
                "td.connect.pass": utils.test_password(),
                "group.id": "7616",
                "auto.offset.reset": "earliest",
                "adapter_ha": "true",
            }
        )
        assert_consumer_can_poll(consumer)
    finally:
        cleanup_topic()


def adapter_ha_connect():
    return taosws.connect(
        host="localhost",
        port=6041,
        user=utils.test_username(),
        password=utils.test_password(),
        adapter_ha="true",
    )


def init_topic():
    cleanup_topic()
    conn = adapter_ha_connect()
    try:
        cursor = conn.cursor()
        statements = [
            "create database test_1782722646",
            "create topic topic_1782722646 as database test_1782722646",
            "use test_1782722646",
            "create table meters(ts timestamp, c1 int) tags(t1 int)",
            "create table tb0 using meters tags(1000)",
            "insert into tb0 values(now, 1)",
        ]
        for statement in statements:
            cursor.execute(statement)
    finally:
        conn.close()


def cleanup_topic():
    for attempt in range(10):
        conn = adapter_ha_connect()
        try:
            conn.execute(f"drop topic if exists topic_1782722646")
            conn.execute(f"drop database if exists test_1782722646")
            return
        except Exception as err:
            if "Topic subscribed cannot be dropped" not in str(err) or attempt == 9:
                raise
            time.sleep(1)
        finally:
            conn.close()


def assert_consumer_can_poll(consumer):
    try:
        consumer.subscribe(["topic_1782722646"])
        message = consumer.poll(timeout=1.0)
        if message is not None:
            for block in message:
                assert block.nrows() >= 0
    finally:
        consumer.unsubscribe()
        consumer.close()
