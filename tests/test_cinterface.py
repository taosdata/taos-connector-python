from taos.cinterface import *
import taos

from dotenv import load_dotenv

from utils import tear_down_database

load_dotenv()


def connect():
    return CTaosInterface().connect()


def teardown_module(module):
    conn = taos.connect()
    db_name = "pytest_taos_stmt"
    tear_down_database(conn, db_name)
    conn.close()


cfg = dict(
    host='localhost',
    user='root',
    password='taosdata',
)


def test_taos_get_client_info():
    print(taos_get_client_info())
    assert taos_get_client_info() is not None


def test_taos_connect_auth():
    if not taos.IS_V3:
        return
    taos_connect_auth(
        host='localhost',
        user='root',
        auth='dcc5bed04851fec854c035b2e40263b6',
    )


def test_taos_connect():
    taos_connect(
        host='localhost',
        user='root',
        password='taosdata',
    )


def test_taos_use_result():
    c = taos_connect(**cfg)
    sql = 'show databases'
    r = taos_query(c, sql)
    try:
        taos_use_result(r)
    except Exception as e:
        print(e)


def test_taos_load_table_info():
    c = taos_connect(**cfg)
    taos_load_table_info(c, 'information_schema.ins_dnodes')


def test_taos_validate_sql():
    c = taos_connect(**cfg)
    sql = 'show databases'
    taos_validate_sql(c, sql)


def test_taos_stmt_errstr():
    if not taos.IS_V3:
        return
    conn = taos.connect(**cfg)

    dbname = "pytest_taos_stmt"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
        )
        # conn.load_table_info("log")

        stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

        stmt.set_tbname("test")

        params = taos.new_bind_params(16)
        params[0].timestamp(1626861392589, taos.PrecisionEnum.Milliseconds)
        params[1].bool(True)
        params[2].tinyint(None)
        params[3].tinyint(2)
        params[4].smallint(3)
        params[5].int(4)
        params[6].bigint(5)
        params[7].tinyint_unsigned(6)
        params[8].smallint_unsigned(7)
        params[9].int_unsigned(8)
        params[10].bigint_unsigned(9)
        params[11].float(10.1)
        params[12].double(10.11)
        params[13].binary("hello")
        params[14].nchar("stmt")
        params[15].timestamp(1626861392589, taos.PrecisionEnum.Milliseconds)

        stmt.bind_param(params)
        stmt.execute()

        assert stmt.affected_rows == 1

        result = conn.query("select * from log")
        row = result.next()
        print(row)
        assert row[2] is None
        for i in range(3, 11):
            assert row[i] == i - 1
        # float == may not work as expected
        # assert row[10] == c_float(10.1)
        assert row[12] == 10.11
        assert row[13] == "hello"
        assert row[14] == "stmt"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_insert")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err
