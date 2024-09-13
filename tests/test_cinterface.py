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
    host="localhost",
    user="root",
    password="taosdata",
)


def test_taos_get_client_info():
    info = taos_get_client_info()
    assert info is not None
    print("pass test_taos_connect_auth")

def test_taos_connect_auth():
    if not taos.IS_V3:
        return
    conn = taos_connect_auth(
        host="localhost",
        user="root",
        auth="dcc5bed04851fec854c035b2e40263b6",
    )
    assert conn is not None
    print("pass test_taos_connect_auth")


def test_taos_connect():
    conn = taos_connect(
        host="localhost",
        user="root",
        password="taosdata",
    )
    assert conn is not None
    print("pass test_taos_connect")


def test_taos_use_result():
    c = taos_connect(**cfg)
    sql = "show databases"
    r = taos_query(c, sql)
    try:
        fields = taos_use_result(r)
        assert isinstance(fields, list)
        print("pass test_taos_use_result")
    except Exception as e:
        print(e)
        raise e


def test_taos_load_table_info():
    c = taos_connect(**cfg)
    taos_load_table_info(c, "information_schema.ins_dnodes")
    print("pass test_taos_load_table_info")


def test_taos_validate_sql():
    c = taos_connect(**cfg)
    sql = "show databases"
    msg = taos_validate_sql(c, sql)
    assert msg is None
    print("pass test_taos_validate_sql")


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
        # print(row)
        assert len(row) == 16
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


def test_taos_use_result():
    c = taos_connect(**cfg)
    sql = "show databases"
    r = taos_query(c, sql)
    try:
        fields = taos_use_result(r)
        assert isinstance(fields, list)
        print("pass test_taos_use_result")
    except Exception as e:
        print(e)
        raise e

############################################ stmt2 begin ############################################

def execute_sql(conn, sql):
    res = taos_query(conn, sql)
    taos_free_result(res)


def test_taos_stmt2_init_without_option():
    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)
    assert stmt2 is not None
    taos_stmt2_close(stmt2)
    taos_close(conn)
    print("pass test_taos_stmt2_init_without_option")


def test_taos_stmt2_init_with_option():
    conn = taos_connect(**cfg)
    option = taos.TaosStmt2Option(reqid=1, single_stb_insert=True, single_table_bind_once=False).get_impl()
    stmt2 = taos_stmt2_init(conn, option)
    assert stmt2 is not None
    taos_stmt2_close(stmt2)
    taos_close(conn)
    print("pass test_taos_stmt2_init_with_option")


def test_taos_stmt2_insert():
    from taos.bind2 import TaosStmt2Bind, new_stmt2_binds, new_bindv

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)

    dbname = "stmt2"
    stbname = "meters"
    execute_sql(conn, "drop database if exists %s" % dbname)
    execute_sql(conn, "create database if not exists %s" % dbname)
    taos_select_db(conn, dbname)

    sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    execute_sql(conn, sql)

    sql = f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)"
    taos_stmt2_prepare(stmt2, sql)

    # prepare data
    tbanmes = ["d1", "d2", "d3"]
    tags = [
        ["grade1", 1],
        ["grade1", 2],
        ["grade1", 3]
    ]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99]
        ],
            # class 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299]
        ],
            # class 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399]

        ]
    ]

    cnt_tbls = 3
    cnt_tags = 2
    cnt_cols = 4
    cnt_rows = 5
    # new_stmt2_binds(cnt_cols)

    # tags
    stmt2_tags = []
    for tag_list in tags:
        n = len(tag_list)
        assert n == cnt_tags
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].binary(tag_list[0])
        binds[1].int(tag_list[1])
        stmt2_tags.append(binds)
    #

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].timestamp(data_list[0])
        binds[1].binary(data_list[1])
        binds[2].bool(data_list[2])
        binds[3].int(data_list[3])
        stmt2_cols.append(binds)
    #

    bindv = new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)

    taos_stmt2_bind_param(stmt2, bindv.get_address(), -1)

    affected_rows = taos_stmt2_exec(stmt2)
    assert affected_rows == cnt_tbls * cnt_rows

    taos_stmt2_close(stmt2)
    taos_close(conn)

    print("pass test_taos_stmt2_insert")



# def test_taos_stmt2_error():
#     conn = taos_connect(**cfg)
#     stmt2 = taos_stmt2_init(conn, None)
#     result = taos_stmt2_error(stmt)
#     self.assertIsNone(result)
#     print("pass test_taos_stmt2_error_no_error")
#


############################################ stmt2 end ############################################
