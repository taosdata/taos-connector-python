import pandas
from sqlalchemy import create_engine

engine = create_engine("taos://root:taosdata@localhost:6030/log")
res = pandas.read_sql("select * from logs", engine)

print(res)

# import pandas
# import taos

# engine = taos.connect()
# res = pandas.read_sql("select * from log.logs", engine)

# print(res)
