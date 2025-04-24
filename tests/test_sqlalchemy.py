import taos
import taosrest
import datetime
from sqlalchemy import types as sqltypes
from sqlalchemy import create_engine
from sqlalchemy import inspect
from dotenv import load_dotenv
load_dotenv()


def insertData(conn):
    if conn is None:
       c = taos.connect()
    else:
       c = conn
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)")
    c.execute("insert into test.d0 using test.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)")
    c.execute("insert into test.d1 using test.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)")
    c.execute("create table test.ntb(ts timestamp, age int)")
    c.execute("insert into test.ntb values(now, 23)")


# compare list
def checkListEqual(list1, list2, tips):
    if list1 != list2:
        print(f"{tips} failed. two list item not equal. list1={list1} list2={list2}")
        raise(BaseException("list not euqal."))

# check result
def checkResultEqual(result1, result2, tips):
    if result1 != result2:
        print(f"{tips} failed. result not equal. result1={result1} result2={result2}")
        raise(BaseException("result not euqal."))

# check baisc function
def checkBasic(conn, inspection):
    # get schema names
    databases = inspection.get_schema_names()
    if "test" not in databases:
        print(f"test not in {databases}")
        raise(BaseException("get_schema_names failed."))
    
    # get table names
    tables = ['meters','ntb']
    checkListEqual(inspection.get_table_names("test"), tables, "check get_table_names()")

    # get_columns
    cols2 = [
        {'name': 'ts', 'type': inspection.dialect._resolve_type("TIMESTAMP")}, 
        {'name': 'c1', 'type': inspection.dialect._resolve_type("INT")}, 
        {'name': 'c2', 'type': inspection.dialect._resolve_type("DOUBLE")}, 
        {'name': 't1', 'type': inspection.dialect._resolve_type("INT")}
    ]
    cols1 = inspection.get_columns("meters", "test")
    for i in range(len(cols1)):
        cname1 = cols1[i]['name']
        ctype1 = cols1[i]['type']
        cname2 = cols2[i]['name']
        ctype2 = cols2[i]['type']

        if cname1 != cname2:
            print(f"two name diff, name1={cname1} name2={cname2}")
            raise("name diff")

        if type(ctype1) != ctype2:
            print(f"two type diff, type1={ctype1} | {type(ctype1)} type2={str(ctype2)} | {type(ctype2)}")
            raise("type diff")

    # has tabled
    checkResultEqual(inspection.has_table("meters", "test"), True, "check has_table()")
    # has_schema
    checkResultEqual(inspection.dialect.has_schema(conn, "test"), True, "check has_schema()")

    # get_indexes
    print("inspection.get_indexes", inspection.get_indexes("test.meters"))

    res = conn.execute("select * from test.meters")
    print("res", res.fetchall())

    # import_dbapi
    print("inspection.dialect.import_dbapi", inspection.dialect.import_dbapi())

    # _resolve_type
    print("inspection.dialect._resolve_type", inspection.dialect._resolve_type("int"))

    conn.close()


# taos
def test_read_from_sqlalchemy_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertData(conn)
    inspection = inspect(engine)
    checkBasic(conn, inspection)

# taosws
def test_read_from_sqlalchemy_taosws():
    try:
        import taosws
    except ImportError:
        return
    engine = create_engine("taosws://root:taosdata@localhost:6041?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertData(None)
    inspection = inspect(engine)
    checkBasic(conn, inspection)


# taosrest
def test_read_from_sqlalchemy_taosrest():
    if not taos.IS_V3:
        return
    engine = create_engine("taosrest://root:taosdata@localhost:6041?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertData(conn)
    inspection = inspect(engine)
    checkBasic(conn, inspection)


# main test
if __name__ == "__main__":
    print("hello, test sqlalcemy db api. do nothing\n")
    test_read_from_sqlalchemy_taos()
    print("Test taos api ..................................... [OK]\n")
    test_read_from_sqlalchemy_taosrest()
    print("Test taosrest api ................................. [OK]\n")
    test_read_from_sqlalchemy_taosws()
    print("Test taosws api ................................... [OK]\n")
    
