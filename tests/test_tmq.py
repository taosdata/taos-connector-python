import threading
from time import sleep

import taos
from taos.tmq import *


class insertThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print("Start insert data")
        conn = taos.connect()
        conn.select_db("tmq_test")
        conn.execute("insert into tb1 values (now, true,1,1,1,1,1,1,1,1,1,1,1,'1','1')")
        print("Finish insert data")


class subscribeThread(threading.Thread):
    def __init__(self, tmq):
        threading.Thread.__init__(self)
        self.tmq = tmq

    def run(self):
        print("Start Subscribe")
        while 1:
            res = self.tmq.poll(1000)
            if res:
                topic = res.get_topic_name()
                db = res.get_db_name()

                assert topic == "topic1"
                assert db == "tmq_test"

                break
        print("Finish Subscribe")


class ConsumerThread(threading.Thread):
    def __init__(self, consumer: Consumer):
        threading.Thread.__init__(self)
        self.consumer = consumer

    def run(self) -> None:
        print("Starting subscribe")
        while True:
            res = self.consumer.poll(1000)
            if not res:
                continue
            assert res.error() is None

            topic = res.topic()
            assert topic == "topic1"

            break
        print("Finished Subscribe")


def pre_test_tmq(precision: str):
    conn = taos.connect()
    conn.execute("drop topic if exists topic1")
    conn.execute("drop database if exists tmq_test")
    if len(precision) > 0:
        conn.execute(
            "create database if not exists tmq_test precision '{}' wal_retention_period 3600".format(precision)
        )
    else:
        conn.execute("create database if not exists tmq_test wal_retention_period 3600")
    conn.select_db("tmq_test")
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, \
        c1 bool, c2 tinyint unsigned, c3 smallint unsigned, c4 int unsigned, \
        c5 bigint unsigned, c6 tinyint, c7 smallint, c8 tinyint, c9 bigint, c10 float, \
        c11 double, c12 timestamp, c13 varchar(8), c14 nchar(8)) tags \
        (t1 bool, t2 tinyint unsigned, t3 smallint unsigned, t4 int unsigned, \
        t5 bigint unsigned, t6 tinyint, t7 smallint, t8 tinyint, t9 bigint, t10 float, \
        t11 double, t12 timestamp, t13 varchar(8), t14 nchar(8))"
    )
    conn.execute("create table if not exists tb1 using stb1 tags (true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1')")
    print("========start create topic")
    conn.execute(
        "create topic if not exists topic1 as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14 from stb1"
    )


def after_ter_tmq():
    conn = taos.connect()
    # drop database and topic
    conn.execute("drop topic if exists topic1")
    conn.execute("drop database if exists tmq_test")


def test_consumer_with_precision():
    tmq_consumer_with_precision('ms')
    tmq_consumer_with_precision('us')
    tmq_consumer_with_precision('ns')


def tmq_consumer_with_precision(precision: str):
    if not IS_V3:
        return
    pre_test_tmq(precision)

    consumer = Consumer({
        "group.id": "1",
        "td.connect.user": "root",
        "td.connect.pass": "taosdata",
        "msg.with.table.name": "true"
    })
    consumer.subscribe(["topic1"])

    sThread = ConsumerThread(consumer)
    iThread = insertThread()

    sThread.start()
    sleep(2)
    iThread.start()

    iThread.join()
    sThread.join()

    print("====== finish test, start clean")
    print("====== unsubscribe topic")
    consumer.unsubscribe()

    after_ter_tmq()


if __name__ == "__main__":
    test_consumer_with_precision()
