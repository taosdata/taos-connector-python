from taosws import Consumer
import taosws
import time
import pytest
import os
import utils


def init_topic():
    conn = taosws.connect()
    cursor = conn.cursor()
    statements = [
        "drop topic if exists test_topic_1",
        "drop database if exists test_topic_1",
        "create database test_topic_1 wal_retention_period 3600",
        "create topic test_topic_1 with meta as database test_topic_1",
        "use test_topic_1",
        "create table meters(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 int)",
        "create table tb0 using meters tags(1000)",
        "create table tb1 using meters tags(NULL)",
        """insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, 
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',
            254, 65534, 1, 1)""",
    ]
    for statement in statements:
        cursor.execute(statement)


def test_comsumer():
    init_topic()
    conf = {
        "td.connect.websocket.scheme": "ws",
        "group.id": "0",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)

    consumer.subscribe(["test_topic_1"])

    while 1:
        message = consumer.poll(timeout=1.0)
        if message:
            id = message.vgroup()
            topic = message.topic()
            database = message.database()

            for block in message:
                nrows = block.nrows()
                ncols = block.ncols()
                for row in block:
                    print(row)
                values = block.fetchall()
                print(nrows, ncols)
        else:
            break

    consumer.unsubscribe()
    consumer.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_report_connector_info():
    conn = taosws.connect()
    cursor = conn.cursor()
    cursor.execute("drop topic if exists topic_1784703210")
    cursor.execute("drop database if exists test_1784703210")
    cursor.execute("create database test_1784703210")
    cursor.execute("create topic topic_1784703210 as database test_1784703210")
    cursor.execute("create table test_1784703210.t0 (ts timestamp, c1 int)")
    cursor.execute("insert into test_1784703210.t0 values(now, 1)")

    connector_info = utils.get_connector_info()
    print("connector_info:", connector_info)

    consumer1 = Consumer(conf={
        "td.connect.websocket.scheme": "ws",
        "td.connect.ip": "localhost",
        "td.connect.port": 6041,
        "td.connect.user": utils.test_username(),
        "td.connect.pass": utils.test_password(),
        "group.id": "3731",
        "client.id": "3731",
    })
    consumer1.subscribe(["topic_1784703210"])
    time.sleep(2)
    res = conn.query("show connections")
    assert any(connector_info == col for row in res for col in row)
    consumer1.unsubscribe()

    consumer2 = Consumer(dsn="ws://localhost:6041?group.id=3730")
    consumer2.subscribe(["topic_1784703210"])
    time.sleep(2)
    res = conn.query("show connections")
    assert any(connector_info == col for row in res for col in row)
    consumer2.unsubscribe()

    time.sleep(3)

    conn.execute("drop topic if exists topic_1784703210")
    conn.execute("drop database if exists test_1784703210")
    conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_connect_with_user_app():
    conn = taosws.connect()
    cursor = conn.cursor()
    cursor.execute("drop topic if exists topic_1784700309")
    cursor.execute("drop database if exists test_1784700309")
    cursor.execute("create database test_1784700309")
    cursor.execute("create topic topic_1784700309 as database test_1784700309")
    cursor.execute("create table test_1784700309.t0 (ts timestamp, c1 int)")
    cursor.execute("insert into test_1784700309.t0 values(now, 1)")

    user_app = "test-python-ws-tmq"
    consumer = Consumer(
        conf={
            "td.connect.websocket.scheme": "ws",
            "td.connect.ip": "localhost",
            "td.connect.port": 6041,
            "td.connect.user": utils.test_username(),
            "td.connect.pass": utils.test_password(),
            "group.id": "3732",
            "client.id": "3732",
            "user_app": user_app,
        }
    )
    consumer.subscribe(["topic_1784700309"])

    deadline = time.monotonic() + 10
    while True:
        res = conn.query("show connections")
        if any(user_app == col for row in res for col in row):
            break
        if time.monotonic() >= deadline:
            pytest.fail(f"user_app {user_app!r} was not found in show connections")
        time.sleep(0.5)
 
    consumer.unsubscribe()

    time.sleep(3)

    conn.execute("drop topic if exists topic_1784700309")
    conn.execute("drop database if exists test_1784700309")
    conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_connect_dsn_with_user_app():
    conn = taosws.connect()
    cursor = conn.cursor()
    cursor.execute("drop topic if exists topic_1784700573")
    cursor.execute("drop database if exists test_1784700573")
    cursor.execute("create database test_1784700573")
    cursor.execute("create topic topic_1784700573 as database test_1784700573")
    cursor.execute("create table test_1784700573.t0 (ts timestamp, c1 int)")
    cursor.execute("insert into test_1784700573.t0 values(now, 1)")

    user_app = "test-python-ws-tmq-dsn"
    consumer = Consumer(dsn=f"ws://localhost:6041?group.id=3733&client.id=3733&user_app={user_app}")
    consumer.subscribe(["topic_1784700573"])

    deadline = time.monotonic() + 10
    while True:
        res = conn.query("show connections")
        if any(user_app == col for row in res for col in row):
            break
        if time.monotonic() >= deadline:
            pytest.fail(f"user_app {user_app!r} was not found in show connections")
        time.sleep(0.5)

    consumer.unsubscribe()

    time.sleep(3)

    conn.execute("drop topic if exists topic_1784700573")
    conn.execute("drop database if exists test_1784700573")
    conn.close()
