from taos._objects import TaosConnection, TaosConsumer

def init_tmq_env(db, topic):
    conn = TaosConnection(host="localhost")
    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))
    conn.execute("create database if not exists {} WAL_RETENTION_PERIOD 3600000 ".format(db))
    conn.select_db(db)
    conn.execute("create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
    conn.execute("create table if not exists tb1 using stb1 tags(1)")
    conn.execute("create table if not exists tb2 using stb1 tags(2)")
    conn.execute("create table if not exists tb3 using stb1 tags(3)")
    conn.execute("create topic if not exists {} as select ts, c1, c2, c3 from stb1".format(topic))
    conn.execute("insert into tb1 values (now-2s, 1, 1.0, 'tmq test')")
    conn.execute("insert into tb2 values (now-1s, 2, 2.0, 'tmq test')")
    conn.execute("insert into tb3 values (now, 3, 3.0, 'tmq test')")


if __name__ == '__main__':
    # init env
    init_tmq_env("tmq_test", "tmq_test_topic")
    consumer = TaosConsumer(
        {
            "group.id": "test",
            'td.connect.ip': "localhost",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "msg.with.table.name": "true",
            "enable.auto.commit": "true",
        }
    )
    consumer.subscribe(["tmq_test_topic"])
    consumer.set_auto_commit_cb(print, print)

    while True:
        res = consumer.poll(10)
        if not res:
            consumer.close()
            break
        err = res.error()
        if err is not None:
            raise err
        val = res.value()

        for block in val:
            print(block.fetchall())
