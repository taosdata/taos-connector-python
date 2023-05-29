import taosws
from taosws import Consumer


def setup():
    conn = taosws.connect('taosws://root:taosdata@localhost:6041')
    cursor = conn.cursor()
    statements = [
            "drop topic if exists ws_tmq_meta",
            "drop database if exists ws_tmq_meta",
            "create database ws_tmq_meta wal_retention_period 3600",
            "create topic ws_tmq_meta with meta as database ws_tmq_meta",
            "use ws_tmq_meta",

            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 int)",

            "create table tb0 using stb1 tags(1000)",
            "create table tb1 using stb1 tags(NULL)",
            """insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, 
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',
            254, 65534, 1, 1)""",

            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",

            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",

            "create table `table` (ts timestamp, v int)",

            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",

            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",

            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",

        ]
    for statement in statements:
        # print(statement)
        cursor.execute(statement)

def test_tmq():
    setup()
    conf = {
        "td.connect.websocket.scheme": "ws",
        "group.id": "0",
    }
    consumer = Consumer(conf)

    consumer.subscribe(["ws_tmq_meta"])

    while 1:
        message = consumer.poll(timeout=1.0)
        if message:
            id = message.vgroup()
            topic = message.topic()
            database = message.database()
            print(f"vgroup: {id}, topic: {topic}, database: {database}")

            for block in message:
                nrows = block.nrows()
                ncols = block.ncols()
                for row in block:
                    print(row)
                values = block.fetchall()
                print(f"nrows: {nrows}, ncols: {ncols}, values: {values}")
                
            # consumer.commit(message)
        else:
            break

    consumer.unsubscribe()


def show_env():
    import os
    print('-' * 40)
    print('TAOS_LIBRARY_PATH: ', os.environ.get('TAOS_LIBRARY_PATH'))
    print('TAOS_CONFIG_DIR: ', os.environ.get('TAOS_CONFIG_DIR'))
    print('TAOS_C_CLIENT_VERSION: ', os.environ.get('TAOS_C_CLIENT_VERSION'))
    print('TAOS_C_CLIENT_VERSION_STR: ', os.environ.get('TAOS_C_CLIENT_VERSION_STR'))
    print('taosws.__version__', taosws.__version__)
    print('-' * 40)


if __name__ == '__main__':
    show_env()
    test_tmq()
