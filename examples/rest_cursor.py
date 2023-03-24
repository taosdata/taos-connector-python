import taosrest

# all parameters are optional
conn = taosrest.connect(url="http://localhost:6041",
                        user="root",
                        password="taosdata")
cursor = conn.cursor()

cursor.execute_with_req_id("show databases")
results: list[tuple] = cursor.fetchall()
for row in results:
    print(row)