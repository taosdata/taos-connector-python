from taos._objects import TaosConnection

conn = TaosConnection(host="localhost")
cursor = conn.cursor()

cursor.execute("show databases")
results = cursor.fetchall()
for row in results:
    print(row)

conn.close()
