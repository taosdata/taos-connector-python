import pytest

from taos.bind import *
from taos.cinterface import *


@pytest.fixture
def conn():
    return CTaosInterface().connect()


def test_simple(conn, caplog):
    dbname = "pytest_ctaos_simple"
    try:
        res = taos_query(conn, "create database if not exists %s" % dbname)
        taos_free_result(res)

        taos_select_db(conn, dbname)

        res = taos_query(
            conn,
            "create table if not exists log(ts timestamp, level tinyint, content binary(100), ipaddr binary(134))",
        )
        taos_free_result(res)

        res = taos_query(conn, "insert into log values(now, 1, 'hello', 'test')")
        taos_free_result(res)

        res = taos_query(conn, "select level,content,ipaddr from log limit 1")

        fields = taos_fetch_fields_raw(res)
        field_count = taos_field_count(res)

        fields = taos_fetch_fields(res)
        for field in fields:
            print(field)

        # field_lengths = taos_fetch_lengths(res, field_count)
        # if not field_lengths:
        #     raise "fetch lengths error"

        row = taos_fetch_row_raw(res)
        rowstr = taos_print_row(row, fields, field_count)
        assert rowstr == "1 hello test"

        row, num = taos_fetch_row(res, fields)
        print(row)
        taos_free_result(res)
        taos_query(conn, "drop database if exists " + dbname)
        taos_close(conn)
    except Exception as err:
        taos_query(conn, "drop database if exists " + dbname)
        raise err


def test_stmt(conn, caplog):
    dbname = "pytest_ctaos_stmt"
    try:
        res = taos_query(conn, "drop database if exists %s" % dbname)
        taos_free_result(res)
        res = taos_query(conn, "create database if not exists %s" % dbname)
        taos_free_result(res)

        taos_select_db(conn, dbname)

        res = taos_query(
            conn,
            "create table if not exists log(ts timestamp, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100))",
        )
        taos_free_result(res)

        stmt = taos_stmt_init(conn)

        taos_stmt_prepare(stmt, "insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

        params = new_bind_params(14)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].tinyint(None)
        params[2].tinyint(2)
        params[3].smallint(3)
        params[4].int(4)
        params[5].bigint(5)
        params[6].tinyint_unsigned(6)
        params[7].smallint_unsigned(7)
        params[8].int_unsigned(8)
        params[9].bigint_unsigned(9)
        params[10].float(10.1)
        params[11].double(10.11)
        params[12].binary("hello")
        params[13].nchar("stmt")
        taos_stmt_bind_param(stmt, params)
        taos_stmt_add_batch(stmt)
        taos_stmt_execute(stmt)

        res = taos_query(conn, "select * from log limit 1")

        fields = taos_fetch_fields(res)
        filed_count = taos_field_count(res)

        row = taos_fetch_row_raw(res)
        rowstr = taos_print_row(row, fields, filed_count, 100)

        taos_free_result(res)
        taos_query(conn, "drop database if exists " + dbname)
        taos_close(conn)

        assert rowstr == "1626861392589 NULL 2 3 4 5 6 7 8 9 10.100000 10.110000 hello stmt"
    except Exception as err:
        taos_query(conn, "drop database if exists " + dbname)
        raise err

