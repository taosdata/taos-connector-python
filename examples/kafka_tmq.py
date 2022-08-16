import taos
from taos.tmq import *

conn = taos.connect()

print("init")
conn.execute("drop topic if exists topic_ctb_column")
conn.execute("drop database if exists py_tmq")
conn.execute("create database if not exists py_tmq vgroups 2")
conn.select_db("py_tmq")
conn.execute("create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
conn.execute("create table if not exists tb1 using stb1 tags(1)")
conn.execute("create table if not exists tb2 using stb1 tags(2)")
conn.execute("create table if not exists tb3 using stb1 tags(3)")

print("create topic")
conn.execute("drop topic if exists topic_ctb_column")
conn.execute("create topic if not exists topic_ctb_column as select ts, c1, c2, c3 from stb1")

print("build consumer")
consumer = TaosConsumer('topic_ctb_column', group_id='123')
for msg in consumer:
    for row in msg:
        print(row)