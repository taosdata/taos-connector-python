from taosws import Consumer
import taosws
import time
import pytest
import os


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


@pytest.mark.skip
def test_report_connector_info():
    test = os.getenv("TEST_TD_3360")
    if test is not None:
        return

    init_topic()
    conn = taosws.connect()

    def find_connections():
        time.sleep(2)
        res = conn.query("show connections")
        found = False
        for row in res:
            connector_info = row[row.__len__() - 1]
            if "python-ws" in str(connector_info):
                found = True
                print("connector_info:", connector_info)
        assert found

    conf = {
        "group.id": "10",
    }
    consumer1 = Consumer(conf)
    consumer1.subscribe(["test_topic_1"])
    find_connections()

    consumer2 = Consumer(dsn="ws://localhost:6041?group.id=10")
    consumer2.subscribe(["test_topic_1"])
    find_connections()

    time.sleep(2)

    consumer1.unsubscribe()
    consumer2.unsubscribe()
    consumer1.close()
    consumer2.close()
    conn.close()
