import taos

conn = taos.connect(host='127.0.0.1',
                    user='root',
                    password='taosdata',
                    database='log',
                    config='/etc/taos',
                    timezone='Asia/Shanghai')
cursor = conn.cursor()

sql = "select * from log.log limit 10"
cursor.execute(sql)
for row in cursor:
    print(row)
