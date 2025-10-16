import taos
import taosrest
import datetime
from sqlalchemy import types as sqltypes
from sqlalchemy import create_engine
from sqlalchemy import inspect
from dotenv import load_dotenv
from sqlalchemy import text
load_dotenv()

taos.log.setting(True, True, True, True, True, True)

def prepare(conn, dbname, stbname, ntb1, ntb2):
    conn.execute("drop database if exists %s" % dbname)
    conn.execute("create database if not exists %s precision 'ms' " % dbname)
    # stable
    sql = f"create table if not exists {dbname}.{stbname}(ts timestamp, name binary(32), sex bool, score int, remarks blob) tags(grade nchar(8), class int)"
    conn.execute(sql)
    # normal table
    sql = f"create table if not exists {dbname}.{ntb1} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks blob)"
    conn.execute(sql)
    sql = f"create table if not exists {dbname}.{ntb2} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks blob)"
    conn.execute(sql)

def test_stmt2_query():
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    dbname  = "stmt2"
    stbname = "meters"
    ntb1    = "ntb1"
    ntb2    = "ntb2"
    # sql1 = f"select * from {dbname}.d2 where name in (?) or score > ? ;"
    sql1 = f"select * from {dbname}.d2 where score > ? and score < ?;"
    try:
        # prepare
        prepare(conn, dbname, stbname, ntb1, ntb2)

        # prepare
        # stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?,?) values(?,?,?,?)")
        # insert_bind_param_with_tables(conn, stmt2, dbname, stbname)
        # insert_bind_param(conn, stmt2, dbname, stbname)
        # stmt2.close()
        # print("insert bind & execute ......................... ok\n")

        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.000', 'Mary2', false, 298, 'XXX')")
        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.001', 'Tom2', true, 280, 'YYY')")
        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.002', 'Jack2', true, 260, 'ZZZ')")
        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.003', 'Jane2', false, 2100, 'WWW')")
        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.004', 'alex2', true, 299, 'ZZZ')")
        conn.execute(f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.005', NULL, false, NULL, 'WWW')")
        datas   = [
            # class 1
            [
                # where name in ('Tom2','alex2') or score > 1000;"
                [280],
                [1000]
            ]
        ]

        result = conn.execute(sql1, datas)
        # print(f"result: {result}")
        # for row in result:
        #     print(f" result rows = {row} \n")
        print("result:", result.fetchall())

        conn.close()
        print("test_stmt2_query .............................. [passed]\n")

    except Exception as err:
        print("query ......................................... failed\n")
        conn.close()
        raise err

def insertData(conn):
    if conn is None:
       c = taos.connect()
    else:
       c = conn
    c.execute("drop database if exists test")
    c.execute("create database if not exists test")
    c.execute("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)")
    c.execute("insert into test.d0 using test.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)")
    c.execute("insert into test.d1 using test.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)")
    c.execute("create table test.ntb(ts timestamp, age int)")
    c.execute("insert into test.ntb values(now, 23)")

def insertStmtData(conn):
    if conn is None:
         raise(BaseException("conn is null failed."))

    conn.execute(text("drop database if exists test"))
    conn.execute(text("create database if not exists test"))
    conn.execute(text("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)"))

    datas   = [
        [1626861392589, 7, 1.1, 1, "tb1"],
        [1626861392590, 8, 1.2, 2, "tb2"],
        [1626861392591, 9, 1.3, 3, "tb3"],
        [1626861392592, 10, 1.4, 4, "tb4"],
        [1626861392593, 11, 1.4, 4, "tb4"],
        [1626861392584, 7, 1.1, 1, "tb1"],
        [1626861392595, 8, 1.2, 2, "tb2"],
        [1626861392596, 9, 1.3, 3, "tb3"],
        [1626861392597, 10, 1.4, 4, "tb4"],
        [1626861392598, 11, 1.4, 4, "tb4"]
    ]
    rows = conn.execute("insert into test.meters (ts, c1, c2, t1, tbname) values (?, ?, ?, ?, ?)", datas)
    print(f"inserted data done, rows={rows}")
    data = [
        [
            [1626861392589],
            [1626861392598]
        ]
    ]
    result = conn.execute("select * from test.meters where ts > ? and ts < ?", data)
    # print(f"result: {result}")
    # for row in result:
    #     print(f" result rows = {row} \n")
    print("result:", result.fetchall())

def insertStmtSqlalchemyData(conn):
    if conn is None:
         raise(BaseException("conn is null failed."))

    conn.execute(text("drop database if exists test"))
    conn.execute(text("create database if not exists test"))
    conn.execute(text("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)"))

    data = [
        {'ts': 1626861392589, 'c1': 1, 'c2': 2.0, 't1': 1, 'tbname': 'tb1'},
        {'ts': 1626861392590, 'c1': 2, 'c2': 2.5, 't1': 2, 'tbname': 'tb2'},
        {'ts': 1626861392591, 'c1': 3, 'c2': 3.0, 't1': 3, 'tbname': 'tb3'}
    ]	

    sql = text("INSERT INTO test.meters (ts, c1, c2, t1, tbname) VALUES (:ts, :c1, :c2, :t1, :tbname)")
    rows = conn.execute(sql, data)
    print(f"inserted data done, rows={rows}")
    result = conn.execute(text("select * from test.meters where ts > :start and ts < :end"), {'start': 1626861392589, 'end': 1626861392598})
    print(f"result: {result}")
    for row in result:
        print(f" result rows = {row} \n")


# compare list
def checkListEqual(list1, list2, tips):
    if list1 != list2:
        print(f"{tips} failed. two list item not equal. list1={list1} list2={list2}")
        raise(BaseException(f"list not euqal. {list1} != {list2}"))

# check result
def checkResultEqual(result1, result2, tips):
    if result1 != result2:
        print(f"{tips} failed. result not equal. result1={result1} result2={result2}")
        raise(BaseException("result not euqal."))

# check baisc function
def checkBasic(conn, inspection, subTables=['meters','ntb']):
    # get schema names
    databases = inspection.get_schema_names()
    if "test" not in databases:
        print(f"test not in {databases}")
        raise(BaseException("get_schema_names failed."))
    
    # get table names
    tables = subTables
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


#taos
def test_read_from_sqlalchemy_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertData(conn)
    inspection = inspect(engine)
    checkBasic(conn, inspection)

# taos
def test_sqlalchemy_stmt_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertStmtData(conn)
    inspection = inspect(engine)
    checkBasic(conn, inspection, subTables=['meters'])

# taos
def test_sqlalchemy_format_stmt_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    insertStmtSqlalchemyData(conn)
    inspection = inspect(engine)
    checkBasic(conn, inspection, subTables=['meters'])

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


def test_read_from_sqlalchemy_taosws_failover():
    try:
        import taosws
    except ImportError:
        print("taosws not installed, skip test_read_from_sqlalchemy_taosws_failover")
        return

    conn = taos.connect()
    conn.execute("drop database if exists test_1755496227")
    conn.execute("create database test_1755496227")

    try:
        urls = [
            "taosws://",
            "taosws://localhost",
            "taosws://localhost:6041",
            "taosws://localhost:6041/test_1755496227",
            "taosws://root@localhost:6041/test_1755496227",
            "taosws://root:@localhost:6041/test_1755496227",
            "taosws://root:taosdata@localhost:6041/test_1755496227",
            "taosws://root:taosdata@localhost:6041/test_1755496227?hosts=",
            "taosws://root:taosdata@/test_1755496227?hosts=localhost:6041",
            "taosws://root:taosdata@localhost:6041/test_1755496227?hosts=localhost:6041",
            "taosws://root:taosdata@localhost:6041/test_1755496227?hosts=localhost:6041,127.0.0.1:6041",
            "taosws://root:taosdata@localhost:6041/test_1755496227?hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai",
        ]
    
        for url in urls:
            engine = create_engine(url)
            econn = engine.connect()
            econn.close()

        invalid_urls = [
            "taosws://:6041",
            "taosws://:taosdata@localhost:6041/test_1755496227",
        ]

        for url in invalid_urls:
            try:
                engine = create_engine(url)
                econn = engine.connect()
                econn.close()
            except Exception as e:
                print(f"expected error for {url}: {e}")

    finally:
        conn.execute("drop database test_1755496227")
        conn.close()


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
    test_read_from_sqlalchemy_taosws_failover()
    print("Test taosws failover api .......................... [OK]\n")
    
