import taosws
import datetime

conn = taosws.connect()
cursor = conn.cursor()
cursor.execute("select * from test.meters")

# PEP-249 fetchone() method
row = cursor.fetchone()

# PEP-249 fetchmany([size = Cursor.arraysize]) method()
# 1. fetch one block by default, the block size is not predictable.
many_dynamic = cursor.fetchmany()
# 2. fetch exact (maximum) size of rows, the result may be less than the size limit.
many_fixed = cursor.fetchmany(10000)

# all rows in a sequence of tuple
all = cursor.fetchall()

# by dict
cursor.execute("select * from test.meters limit 1")
all_dict = cursor.fetch_all_into_dict()  # same to cursor.fetchallintodict()
#[{'ts': datetime.datetime(2017, 7, 14, 2, 40, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))),
#  'current': 9.880000114440918,
#  'voltage': 114,
#  'phase': 0.3027780055999756,
#  'groupid': 8,
#  'location': 'California.Campbell'}]

# by iterator.
for row in results:
    print(row)

# by fetchone loop
while True:
    row = cursor.fetchone()
    if row:
        print(row)
    else:
        break

results = cursor.fetchall()
for row in results:
    print(row)
