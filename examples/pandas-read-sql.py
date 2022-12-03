import pandas
from sqlalchemy import create_engine

for driver in ("taos", "taosrest"):
    engine = create_engine(f"{driver}://root:taosdata@localhost")
    res = pandas.read_sql("show databases", engine)
    print(res)
