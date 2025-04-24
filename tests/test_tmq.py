import threading
import time
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
        for i in range(50):
            sql = f"insert into tb1 values (now + {1+i}s, true,1,{i},1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')"
            conn.execute(sql)
            print(sql)
            sleep(0.02)
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
        i = 0
        while True:
            print(f"i={i} consumer.poll(6s) ... \n")
            res = self.consumer.poll(6)
            i += 1 
            if not res:
                # throw except
                assert i < 10
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
        precision_str = "precision '{}'".format(precision)
    else:
        precision_str = ""
    
    sql = f"create database if not exists tmq_test {precision_str} wal_retention_period 3600"
    conn.execute(sql)

    conn.select_db("tmq_test")
    sql = '''
        create stable if not exists stb1 (
            ts timestamp,
            c1 bool,
            c2 tinyint unsigned,
            c3 smallint unsigned,
            c4 int unsigned,
            c5 bigint unsigned,
            c6 tinyint,
            c7 smallint,
            c8 tinyint,
            c9 bigint,
            c10 float,
            c11 double,
            c12 timestamp,
            c13 varchar(8),
            c14 nchar(8),
            c15 varbinary(50),
            c16 geometry(512),
            c17 decimal(10,6),
            c18 decimal(24,10)
        ) tags (
            t1 bool,
            t2 tinyint unsigned,
            t3 smallint unsigned,
            t4 int unsigned,
            t5 bigint unsigned,
            t6 tinyint,
            t7 smallint,
            t8 tinyint,
            t9 bigint,
            t10 float,
            t11 double,
            t12 timestamp,
            t13 varchar(8),
            t14 nchar(8)
        )
    '''

    conn.execute(sql)

    conn.execute("create table if not exists tb1 using stb1 tags (true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1')")
    print("======== start create topic")
    conn.execute(
        "create topic if not exists topic1 as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18 from stb1"
    )


def after_ter_tmq():
    conn = taos.connect()
    # drop database and topic
    try:
        conn.execute("drop topic if exists topic1")
        conn.execute("drop database if exists tmq_test")
    except Exception:
        pass

#
# test precison
#
def test_consumer_with_precision():
    tmq_consumer_with_precision("ms")
    tmq_consumer_with_precision("us")
    tmq_consumer_with_precision("ns")


def tmq_consumer_with_precision(precision: str):
    if not IS_V3:
        return
    pre_test_tmq(precision)

    consumer = Consumer(
        {"group.id": "1", "td.connect.user": "root", "td.connect.pass": "taosdata", "msg.with.table.name": "true"}
    )
    consumer.subscribe(["topic1"])
    try:
        sThread = ConsumerThread(consumer)
        iThread = insertThread()

        sThread.start()
        sleep(2)
        iThread.start()

        iThread.join()
        sThread.join()

        print("====== finish test, start clean")
        print("====== unsubscribe topic")
    finally:
        consumer.unsubscribe()
        consumer.close()
        after_ter_tmq()


def test_tmq_assignment():
    if not IS_V3:
        return
    pre_test_tmq("")
    conn = taos.connect()
    conn.select_db("tmq_test")
    conn.execute(
        "insert into t1 using stb1 tags(true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1') values (now-4s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')"
    )
    conn.execute(
        "insert into t2 using stb1 tags(false, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1') values (now-3s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','-9876.123456','-123456789012.0987654321')"
    )
    conn.execute(
        "insert into t3 using stb1 tags(true, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, '2', '2') values (now-2s, true,2,2,2,2,2,2,2,2,2,2,2,'2','2','binary value_1','POINT (3.0 5.0)','5676.123','567890121234.5432109876')"
    )

    consumer = Consumer({"group.id": "1", "auto.offset.reset": "earliest"})
    consumer.subscribe(["topic1"])

    try:
        assignment = consumer.assignment()
        assert assignment[0].offset == 0

        consumer.poll(1)
        message = consumer.poll(1)
        print(f"< before > insert message: {message}")
        if message:
            topic = message.topic()
            database = message.database()
            print(f"topic: {topic}, database: {database}")

            for block in message:
                nrows = block.nrows()
                ncols = block.ncols()
                for row in block:
                    print(row)
                values = block.fetchall()
                print(f"nrows: {nrows}, ncols: {ncols}, values: {values}")

        consumer.commit(message)

        table_num = 10
        data_num = 10
        for i in range(table_num):
            for j in range(data_num):
                conn.execute(
                    f"insert into t{i} using stb1 tags(true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1') values (now, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')"
                )

        message = consumer.poll(5)
        print(f"< after > insert message: {message}")
        if message:
            topic = message.topic()
            database = message.database()
            print(f"topic: {topic}, database: {database}")

            for block in message:
                nrows = block.nrows()
                ncols = block.ncols()
                for row in block:
                    print(row)
                values = block.fetchall()
                print(f"nrows: {nrows}, ncols: {ncols}, values: {values}")

            consumer.commit(message)
        consumer.commit(message)

        assignment = consumer.assignment()

        for assign in assignment:
            print(f"assign: {assign}")

        assert assignment[0].offset >= 0
    finally:
        consumer.unsubscribe()
        consumer.close()
        after_ter_tmq()


def test_tmq_seek():
    if not IS_V3:
        return
    pre_test_tmq("")
    conn = taos.connect()
    conn.select_db("tmq_test")
    conn.execute("insert into tb1 values (now-4s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-3s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-2s, true,2,2,2,2,2,2,2,2,2,2,2,'2','2','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-1s, true,2,2,2,2,2,2,2,2,2,2,2,'2','2','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")

    conf = {
        "group.id": "1", 
        "auto.offset.reset": "earliest",
         # 3.3.6.0 support
        "fetch.max.wait.ms": "3000",
        "min.poll.rows": "128"
    }
    consumer = Consumer(conf)
    consumer.subscribe(["topic1"])
    try:
        assignment = consumer.assignment()
        consumer.poll(1)

        for partition in assignment:
            consumer.seek(partition)

        assignment = consumer.assignment()
        assert assignment[0].offset == 0
    finally:
        consumer.unsubscribe()
        consumer.close()
        after_ter_tmq()


def test_tmq_list_topics():
    if not IS_V3:
        return
    pre_test_tmq("")
    consumer = Consumer({"group.id": "1"})
    consumer.subscribe(["topic1"])
    try:
        topics = consumer.list_topics()
        assert topics == ["topic1"]
    finally:
        consumer.unsubscribe()
        consumer.close()
        after_ter_tmq()


def test_tmq_committed_and_position():
    if not IS_V3:
        return
    pre_test_tmq("")

    conn = taos.connect()
    conn.select_db("tmq_test")
    conn.execute("insert into tb1 values (now-4s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-3s, true,1,1,1,1,1,1,1,1,1,1,1,'1','1','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-2s, true,2,2,2,2,2,2,2,2,2,2,2,'2','2','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")
    conn.execute("insert into tb1 values (now-1s, true,2,2,2,2,2,2,2,2,2,2,2,'2','2','binary value_1','POINT (3.0 5.0)','9876.123456','123456789012.0987654321')")

    consumer = Consumer({"group.id": "1"})
    consumer.subscribe(["topic1"])

    try:
        consumer.poll(1)
        res = consumer.poll(1)
        consumer.commit()
        topic_partitions = consumer.assignment()
        committees = consumer.committed(topic_partitions)
        assert committees[0].offset > 0
        positions = consumer.position(topic_partitions)
        assert positions[0].offset > 0
        assert positions[0].offset == committees[0].offset
    finally:
        consumer.unsubscribe()
        consumer.close()
        after_ter_tmq()

if __name__ == "__main__":
    print("call tst_tmp.py nothing do.\n")
    #test_consumer_with_precision()
