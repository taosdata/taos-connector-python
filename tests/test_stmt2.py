# encoding:UTF-8
from ctypes import *
from datetime import datetime
import pytest
import taos
from taos.statement2 import *

from taos.constants import FieldType

@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return taos.connect()

def checkResultCorrects(conn, tbnames, tags, datas):
    pass

# performace is high
def test_bind_param(conn, stmt2):
    # 
    #  table info , write 5 lines to 3 child tables d0, d1, d2 with super table
    #
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
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
            ["Mary",       "Tom",        "Jack",       "Jane",       "alex"       ],
            [0,            1,            1,            0,            1            ],
            [98,           80,           60,           100,          99           ]
        ],
        # class 2
        [
            # student
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
            ["Mary2",      "Tom2",       "Jack2",       "Jane2",     "alex2"       ],
            [0,            1,            1,             0,           1             ],
            [298,          280,          260,           2100,        299           ]
        ],
        # class 3
        [
            # student
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
            ["Mary3",      "Tom3",       "Jack3",       "Jane3",     "alex3"       ],
            [0,            1,            1,             0,           1             ],
            [398,          380,          360,           3100,        399           ]
        ]
    ]

    # call 
    types = [FieldType.C_TIMESTAMP, FieldType.C_BINARY, FieldType.C_BOOL, FieldType.C_INT]
    stmt2.set_columns_type(types)
    stmt2.bind_param(tbanmes, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, tbanmes, tags, datas)



# performance is lower
def test_bind_param_with_tables(conn, stmt2):

    tbanmes = ["t1", "t2", "t3"]
    tags    = [
        ["grade2", 1],
        ["grade2", 2],
        ["grade2", 3]
    ]

    # prepare data
    datas = [
            # table 1
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary",       "Tom",        "Jack",       "Jane",       "alex"       ],
                [0,            1,            1,            0,            1            ],
                [98,           80,           60,           100,          99           ]
            ],
            # table 2
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary2",      "Tom2",       "Jack2",       "Jane2",     "alex2"       ],
                [0,            1,            1,             0,           1             ],
                [298,          280,          260,           2100,        299           ]
            ],
            # table 3
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary3",      "Tom3",       "Jack3",       "Jane3",     "alex3"       ],
                [0,            1,            1,             0,           1             ],
                [398,          380,          360,           3100,        399           ]
            ]
    ]

    table0 = BindTable(tbanmes[0], tags[0])
    table1 = BindTable(tbanmes[1], tags[1])
    table2 = BindTable(tbanmes[2], tags[2])

    for data in datas[0]:
       table0.add_col_data(data)
    for data in datas[1]:
       table1.add_col_data(data)
    for data in datas[2]:
       table2.add_col_data(data)

    # columns type for stable
    types = [FieldType.C_TIMESTAMP, FieldType.C_BINARY, FieldType.C_BOOL, FieldType.C_INT]
    stmt2.set_columns_type(types)
    stmt2.bind_param_with_tables([table0, table1, table2])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, tbanmes, tags, datas)


def test_stmt2_insert(conn):
    # type: (TaosConnection) -> None
    dbname  = "stmt2"
    stbname = "meters"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
        conn.execute(sql)

        # prepare
        stmt2 = conn.statement2(f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)")
        # conn.load_table_info("log")

        # insert with table
        test_bind_param_with_tables(conn, stmt2)

        # insert with split args
        test_bind_param(conn, stmt2)

        #conn.execute("drop database if exists %s" % dbname)
        print("test_stmt2_insert test successful.")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        raise err



if __name__ == "__main__":
    print("stmt2 test case\n")
    # connect db
    conn = taos.connect()
    print("db connect is successful!\n")

    # test stmt2
    print("stmt2 bind and insert.\n")
    test_stmt2_insert(conn)

    # close
    conn.close()
    print("db disconnect!\n")


