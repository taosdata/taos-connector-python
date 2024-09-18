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
def insert_bind_param(conn, stmt2):
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
    #types = [FieldType.C_TIMESTAMP, FieldType.C_BINARY, FieldType.C_BOOL, FieldType.C_INT]
    #stmt2.set_columns_type(types)
    stmt2.bind_param(tbanmes, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, tbanmes, tags, datas)



# performance is lower
def insert_bind_param_with_tables(conn, stmt2):

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
    #types = [FieldType.C_TIMESTAMP, FieldType.C_BINARY, FieldType.C_BOOL, FieldType.C_INT]
    #stmt2.set_columns_type(types)
    stmt2.bind_param_with_tables([table0, table1, table2])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, tbanmes, tags, datas)


#
# insert
#
def test_stmt2_insert(conn, dbname, stbname):
    try:
        # prepare
        stmt2 = conn.statement2(f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)")

        # insert with table
        #insert_bind_param_with_tables(conn, stmt2)

        # insert with split args
        insert_bind_param(conn, stmt2)

        #conn.execute("drop database if exists %s" % dbname)
        stmt2.close()
        print("test_stmt2_insert test successful.")

    except Exception as err:
        #conn.execute("drop database if exists %s" % dbname)
        raise err


#
#  ------------------------ query -------------------
#
def query_bind_param(conn, stmt2):
    # set param
    #tbanmes = ["d2"]
    tbanmes = None
    tags    = None
    datas   = [
        # class 1
        [
            # where name in ('Tom2','alex2') or score > 1000;"
            ["Tom2"],
            [1000]
        ]
    ]

    # set param
    types = [FieldType.C_BINARY, FieldType.C_INT]
    stmt2.set_columns_type(types)

    # bind
    stmt2.bind_param(tbanmes, tags, datas)


# compare
def compare_result(conn, sql2, res2):
    

    lres1 = []
    lres2 = []
   
    # shor res2
    for row in res2:
        print(f" res2 rows = {row} \n")
        lres2.append(row)

    res1 = conn.query(sql2)
    for row in res1:
        print(f" res1 rows = {row} \n")
        lres1.append(row)

    row1 = len(lres1)
    row2 = len(lres2)
    col1 = len(lres1[0])
    col2 = len(lres2[0])

    # check number
    if row1 != row2:
        err = f"two results row count different. row1={row1} row2={row2}"
        raise(BaseException(err))
    if col1 != col2:
        err = f" two results column count different. col1={col1} col2={col2}"
        raise(BaseException(err))

    for i in range(row1):
        for j in range(col1):
            if lres1[i][j] != lres2[i][j]:
                raise(f" two results data different. i={i} j={j} data1={res1[i][j]} data2={res2[i][j]}\n")

# query
def test_stmt2_query(conn, dbname, stbname):
    sql1 = "select * from d2 where name in (?) or score > ? ;"
    sql2 = "select * from d2 where name in ('Tom2') or score > 1000;"

    try:
        # prepare
        stmt2 = conn.statement2(sql1)
        print("query prepare sql ............................. ok\n")

        # bind
        query_bind_param(conn, stmt2)
        print("query bind param .............................. ok\n")

        # query execute
        stmt2.execute()
        
        # fetch result
        res2 = stmt2.result()

        # check result
        compare_result(conn, sql2, res2)
        print("query check corrent ........................... ok\n")

        #conn.execute("drop database if exists %s" % dbname)
        stmt2.close()
        print("test_stmt2_query test successful.")

    except Exception as err:
        print("query ........................................ failed\n")
        raise err


if __name__ == "__main__":
    print("stmt2 test case\n")
    # connect db
    conn = taos.connect()
    print("db connect is successful!\n")

    # test stmt2
    print("stmt2 bind and insert.\n")

    dbname  = "stmt2"
    stbname = "meters"
    conn.execute("drop database if exists %s" % dbname)
    conn.execute("create database if not exists %s" % dbname)
    conn.select_db(dbname)
    sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    conn.execute(sql)
    

    # insert 
    test_stmt2_insert(conn, dbname, stbname)

    # query
    test_stmt2_query(conn, dbname, stbname)


    # close
    conn.close()
    print("db disconnect!\n")


