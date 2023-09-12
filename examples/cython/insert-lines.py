from taos._objects import TaosConnection
from taos._constants import SmlProtocol, SmlPrecision

conn = TaosConnection(host="localhost")
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
print("inserted")

conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.NOT_CONFIGURED)

tb = conn.query("show tables").fetch_all()[0][0]
print(tb)
result = conn.query("select * from %s" % tb)
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)
conn.close()
