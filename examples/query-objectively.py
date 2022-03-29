import taos

conn = taos.connect()
conn.execute("create database if not exists pytest")

result = conn.query("show databases")
num_of_fields = result.field_count
for field in result.fields:
    print(field)
for row in result:
    print(row)
assert result.row_count > 0
conn.execute("drop database pytest")
