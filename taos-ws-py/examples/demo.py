import taosws
conn = taosws.connect("ws://localhost:6041")

conn.execute_with_req_id("create database if not exists connwspy")
conn.execute_with_req_id("use connwspy")
conn.execute_with_req_id("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute_with_req_id("create table if not exists tb1 using stb tags (1)")
conn.execute_with_req_id("insert into tb1 values (now, 1)")
conn.execute_with_req_id("insert into tb1 values (now, 2)")
conn.execute_with_req_id("insert into tb1 values (now, 3)")

result = conn.query("show databases")
num_of_fields = result.field_count
print(num_of_fields)

for field in result.fields:
    print(field)
for row in result:
    print(row)

conn.execute_with_req_id("drop database if exists connwspy")