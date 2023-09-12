import json
from taos._objects import TaosConnection
import requests

j = requests.get("http://api.coindesk.com/v1/bpi/currentprice.json").json()

# json to sql
ts = j["time"]["updatedISO"]
sql = "insert into"
for id in j["bpi"]:
    bpi = j["bpi"][id]
    sql += ' %s using price tags("%s","%s","%s") values("%s", %lf) ' % (
        id,
        bpi["code"],
        bpi["symbol"],
        bpi["description"],
        ts,
        bpi["rate_float"],
    )

# sql to TDengine
conn = TaosConnection(host="localhost")
conn.execute("drop database if exists bpi")
conn.execute("create database if not exists bpi")
conn.execute("use bpi")
conn.execute(
    "create stable if not exists price (ts timestamp, rate double)"
    + " tags (code binary(10), symbol binary(10), description binary(100))"
)
conn.execute(sql)
result = conn.query("select * from bpi.price")
print(result.fetch_all())
conn.execute("drop database if exists bpi")
conn.close()