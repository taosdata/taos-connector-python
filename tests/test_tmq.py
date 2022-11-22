from time import sleep
import taos
from taos.tmq import *
import threading


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


def test_tmq():
    """This test will test TDengine tmq api validity"""
    if not IS_V3:
        return
    conn = taos.connect()
    conn.execute("create database if not exists tmq_test")
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
    conn.execute("drop topic if exists topic1")
    conn.execute(
        "create topic if not exists topic1 as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14 from stb1"
    )
    conf = TaosTmqConf()
    conf.set("group.id", "1")
    conf.set("td.connect.user", "root")
    conf.set("td.connect.pass", "taosdata")
    conf.set("msg.with.table.name", "true")

    tmq = conf.new_consumer()

    print("====== set topic list")
    topic_list = TaosTmqList()
    topic_list.append("topic1")

    print("====== subscribe topic")
    tmq.subscribe(topic_list)

    print("====== check subscription")
    sub_list = tmq.subscription()
    assert sub_list[0] == "topic1"

    sThread = subscribeThread(tmq)

    iThread = insertThread()

    sThread.start()
    sleep(2)
    iThread.start()

    iThread.join()
    sThread.join()

    print("====== finish test, start clean")
    print("====== unsubscribe topic")
    tmq.unsubscribe()

    # drop database and topic
    conn.execute("drop topic if exists topic1")
    conn.execute("drop database if exists tmq_test")


def tmq_with_precision(precision="ms"):
    conn = taos.connect()
    conn.execute("drop topic if exists topic_1")
    conn.execute("drop database if exists tmq_test")
    conn.execute("create database if not exists tmq_test precision '{}'".format(precision))
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
    conn.execute("insert into tb1 values (now, true,1,1,1,1,1,1,1,1,1,1,1,'1','1')")

    conn.execute("drop topic if exists topic_1")
    conn.execute(
        "create topic if not exists topic_1 as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14 from stb1")
    conf = TaosTmqConf()
    conf.set("group.id", "1")
    conf.set("td.connect.user", "root")
    conf.set("td.connect.pass", "taosdata")
    conf.set("msg.with.table.name", "true")

    tmq = conf.new_consumer()

    topic_list = TaosTmqList()
    topic_list.append("topic_1")
    tmq.subscribe(topic_list)

    res = tmq.poll(1000)
    for row in res:
        print(row)
        assert row[0] is not None
        assert row[2] == 1

    tmq.unsubscribe()

    # drop database and topic
    conn.execute("drop topic if exists topic_1")
    conn.execute("drop database if exists tmq_test")


def test_tmq_with_precision():
    if not IS_V3:
        return
    tmq_with_precision('ms')
    tmq_with_precision('us')
    tmq_with_precision('ns')


if __name__ == "__main__":
    test_tmq()
    test_tmq_with_precision()
