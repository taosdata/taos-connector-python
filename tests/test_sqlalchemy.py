import taos

from sqlalchemy import create_engine
from sqlalchemy import inspect
from dotenv import load_dotenv
load_dotenv()


def test_insert_test_data():
    conn = taos.connect()
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)")
    c.execute("insert into test.d0 using test.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)")
    c.execute("insert into test.d1 using test.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)")

def test_read_from_sqlalchemy_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    inspection = inspect(engine)

    print("inspection.get_schema_names()", inspection.get_schema_names())

    print("inspection.has_table", inspection.has_table("test.meters"))

    # has_schema
    print("inspection.has_schema", inspection.dialect.has_schema(conn, "test"))

    # get_columns
    print("inspection.get_columns", inspection.get_columns("test.meters"))

    # get_indexes
    print("inspection.get_indexes", inspection.get_indexes("test.meters"))

    res = conn.execute("select * from test.meters")
    print("res", res.fetchall())

    # import_dbapi
    print("inspection.dialect.import_dbapi", inspection.dialect.import_dbapi())

    # _resolve_type
    print("inspection.dialect._resolve_type", inspection.dialect._resolve_type("int"))

    conn.close()


def test_read_from_sqlalchemy_taosws():
    try:
        import taosws
    except ImportError:
        return
    if not taos.IS_V3:
        return
    engine = create_engine("taosws://root:taosdata@localhost:6041?timezone=Asia/Shanghai")
    conn = engine.connect()
    inspection = inspect(engine)

    print("inspection.get_schema_names()", inspection.get_schema_names())

    print("inspection.has_table", inspection.has_table("test.meters"))

    # has_schema
    print("inspection.has_schema", inspection.dialect.has_schema(conn, "test"))

    # get_columns
    print("inspection.get_columns", inspection.get_columns("test.meters"))

    # get_indexes
    print("inspection.get_indexes", inspection.get_indexes("test.meters"))

    res = conn.execute("select * from test.meters")
    print("res", res.fetchall())

    # import_dbapi
    print("inspection.dialect.import_dbapi", inspection.dialect.import_dbapi())

    # _resolve_type
    print("inspection.dialect._resolve_type", inspection.dialect._resolve_type("int"))

    conn.close()


def teardown_module(module):
    conn = taos.connect()
    conn.execute("DROP DATABASE IF EXISTS test")
    conn.close()

if __name__ == "__main__":
    print("hello, test sqlalcemy db api.\n")
    test_insert_test_data()
    print("Insert table and data ............................. [OK]\n")
    test_read_from_sqlalchemy_taos()
    print("Test rest api ..................................... [OK]\n")
    test_read_from_sqlalchemy_taosws()
    print("Test websocket api ................................ [OK]\n")
