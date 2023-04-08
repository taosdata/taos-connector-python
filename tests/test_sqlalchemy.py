import taos

from sqlalchemy import create_engine
from sqlalchemy import inspect

from dotenv import load_dotenv

from utils import tear_down_database

load_dotenv()


def test_insert_test_data():
    conn = taos.connect()
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.tb "
              "(ts timestamp, c1 int, c2 double)"
              )
    c.execute("insert into test.tb values "
              "(now, -100, -200.3) "
              "(now+10s, -101, -340.2423424)"
              )


def test_read_from_sqlalchemy_taos():
    if not taos.IS_V3:
        return
    engine = create_engine("taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    inspection = inspect(engine)

    print('inspection.get_schema_names()', inspection.get_schema_names())

    print('inspection.has_table', inspection.has_table('test.tb'))

    # has_schema
    print('inspection.has_schema', inspection.dialect.has_schema(conn, 'test'))

    # get_columns
    print('inspection.get_columns', inspection.get_columns('test.tb'))

    # get_indexes
    print('inspection.get_indexes', inspection.get_indexes('test.tb'))

    res = conn.execute("select * from test.tb")
    print('res', res.fetchall())

    # import_dbapi
    print('inspection.dialect.import_dbapi', inspection.dialect.import_dbapi())

    # _resolve_type
    print('inspection.dialect._resolve_type', inspection.dialect._resolve_type('int'))

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

    print('inspection.get_schema_names()', inspection.get_schema_names())

    print('inspection.has_table', inspection.has_table('test.tb'))

    # has_schema
    print('inspection.has_schema', inspection.dialect.has_schema(conn, 'test'))

    # get_columns
    print('inspection.get_columns', inspection.get_columns('test.tb'))

    # get_indexes
    print('inspection.get_indexes', inspection.get_indexes('test.tb'))

    res = conn.execute("select * from test.tb")
    print('res', res.fetchall())

    # import_dbapi
    print('inspection.dialect.import_dbapi', inspection.dialect.import_dbapi())

    # _resolve_type
    print('inspection.dialect._resolve_type', inspection.dialect._resolve_type('int'))

    conn.close()


def teardown_module(module):
    conn = taos.connect()
    db_name = "test"
    tear_down_database(conn, db_name)
    conn.close()


if __name__ == '__main__':
    test_insert_test_data()
    test_read_from_sqlalchemy_taos()
    test_read_from_sqlalchemy_taosws()
