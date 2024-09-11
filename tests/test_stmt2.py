# encoding:UTF-8
from taos import *

from ctypes import *
from datetime import datetime
import taos
import pytest


@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()

def checkResultCorrects(conn, tbnames, tags, datas):
    pass

def test_stmt_insert(conn):
    # type: (TaosConnection) -> None
    dbname  = "stmt2"
    stbname = "meters"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
        conn.execute(sql)
        # conn.load_table_info("log")

        # 
        #  table info , write 5 lines to 3 child tables d0, d1, d2 with super table
        #
        # 1601481600000



        # prepare data
        tbanmes = ["d1","d2","d3"]
        tags    = [
            ["grade1", 1],
            ["grade1", 2],
            ["grade1", 3]
        ]
        datas   = [
            # class 1
            [
                # student
                [1601481600000, "Mary",  0, 98],
                [1601481600001, "Tom",   1, 80],
                [1601481600002, "Jack",  1, 60],
                [1601481600003, "Jane",  0, 100],
                [1601481600004, "alex",  1, 99]
            ]
            # class 2
            [
                # student
                [1601481600000, "Mary2",  0, 298],
                [1601481600001, "Tom2",   1, 280],
                [1601481600002, "Jack2",  1, 260],
                [1601481600003, "Jane2",  0, 2100],
                [1601481600004, "alex2",  1, 299]
            ]
            # class 3
            [
                # student
                [1601481600000, "Mary3",  0, 398],
                [1601481600001, "Tom3",   1, 380],
                [1601481600002, "Jack3",  1, 360],
                [1601481600003, "Jane3",  0, 3100],
                [1601481600004, "alex3",  1, 399]
            ]
        ]

        stmt2 = conn.statement2(f"insert into {stbname} values(?,?,?,?)")
        stmt2.bind_param(tbanmes, tags, datas)
        stmt2.execute()

        # check correct
        checkResultCorrects(conn, tbanmes, tags, datas)


        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_insert")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err



if __name__ == "__main__":
    print("stmt3 test case\n")
    # connect db
    conn = taos.connect()

    # test stmt2
    test_stmt_insert(conn)

    # close
    conn.close()

