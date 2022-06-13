import taosrest

# all parameters are optional
conn = taosrest.connect(url="http://localhost:6041",
                        user="root",
                        password="taosdata")
cursor = conn.cursor()

cursor.execute("show databases")
results: list[tuple] = cursor.fetchall()
for row in results:
    print(row)

from taosrest import connect, TaosRestConnection, Result

conn: TaosRestConnection = connect()
res: Result = conn.query("show databases")
print("status:", res.status)
print("total rows:", res.rows)
print("column names:", res.head)
print("first row:", res.data[0])

# status: succ
# total rows: 4
# column names: ['name', 'created_time', 'ntables', 'vgroups', 'replica', 'quorum', 'days', 'keep', 'cache(MB)', 'blocks', 'minrows', 'maxrows', 'wallevel', 'fsync', 'comp', 'cachelast', 'precision', 'update', 'status']
# first row: ['test', datetime.datetime(2022, 6, 13, 9, 38, 15, 290000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 1, 1, 1, 1, 10, '3650', 16, 6, 100, 4096, 1, 3000, 2, 0, 'ms', 0, 'ready']
