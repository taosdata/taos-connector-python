from taos._objects import TaosConnection

conn = TaosConnection(host="localhost")
cursor = conn.cursor()

sql = "drop database if exists db"
cursor.execute(sql)
sql = "create database if not exists db"
cursor.execute(sql)
sql = "create table db.tb(ts timestamp, n int, bin binary(10), nc nchar(10))"
cursor.execute(sql)
sql = "insert into db.tb values (1650000000000, 1, 'abc', '北京')"
cursor.execute(sql)
sql = "insert into db.tb values (1650000000001, null, null, null)"
cursor.execute(sql)
sql = "select * from db.tb"
cursor.execute(sql)

for row in cursor:
    print(row)

sql = "drop database if exists db"
cursor.execute(sql)
conn.close()
