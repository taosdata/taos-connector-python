# encoding:UTF-8
from ctypes import *
from datetime import datetime
import pytest
import taos
from taos.statement2 import *
from taos.constants import FieldType
from taos import log
from taos import bind2

@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return taos.connect()


def compareLine(oris, rows):
    n = len(oris)
    if len(rows) != n:
        return False
    for i in range(n):
        if oris[i] != rows[i]:
            if type(rows[i]) == bool:
                if bool(oris[i]) != rows[i]:
                    return False
    return True


def checkResultCorrect(conn, sql, tagsTb, datasTb):
    # column to rows
    log.debug(f"check sql correct: {sql}\n")
    oris = []
    ncol = len(datasTb)
    nrow = len(datasTb[0]) 

    for i in range(nrow):
        row = []
        for j in range(ncol):
           if j == 0:
               # ts column
               c0 = datasTb[j][i]
               if type(c0) is int :
                   row.append(datasTb[j][i])
               else:
                   ts = int(bind2._datetime_to_timestamp(c0, PrecisionEnum.Milliseconds))
                   row.append(ts)
           else:
               row.append(datasTb[j][i])

        if tagsTb is not None:
            row += tagsTb
        oris.append(row)
    
    # fetch all
    lres = []
    log.debug(sql)
    res = conn.query(sql)
    i = 0
    for row in res:
        lrow = list(row)
        lrow[0] = int(lrow[0].timestamp()*1000)
        if compareLine(oris[i], lrow) is False:
            log.info(f"insert data differet. i={i} expect ori data={oris[i]} query from db ={lrow}")
            raise(BaseException("check insert data correct failed."))
        else:
            log.debug(f"i={i} origin data same with get from db\n")
            log.debug(f" origin data = {oris[i]} \n")
            log.debug(f" get from db = {lrow} \n")
        i += 1


def checkResultCorrects(conn, dbname, stbname, tbnames, tags, datas):
    count = len(tbnames)
    for i in range(count):
        if stbname is None:
            sql = f"select * from {dbname}.{tbnames[i]} "
        else:
            sql = f"select * from {dbname}.{stbname} where tbname='{tbnames[i]}' "
            
        checkResultCorrect(conn, sql, tags[i], datas[i])

    print("insert data check correct ..................... ok\n")


def prepare(conn, dbname, stbname):
    conn.execute("drop database if exists %s" % dbname)
    conn.execute("create database if not exists %s precision 'ms' " % dbname)
    conn.select_db(dbname)
    # stable
    sql = f"create table if not exists {dbname}.{stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    conn.execute(sql)
    # normal table
    sql = f"create table if not exists {dbname}.ntb (ts timestamp, name binary(32), sex bool, score int)"
    conn.execute(sql)

# performace is high
def insert_bind_param(conn, stmt2, dbname, stbname):
    # 
    #  table info , write 5 lines to 3 child tables d0, d1, d2 with super table
    #
    tbanmes = ["d1","d2","d3"]
    
    tags    = [
        ["grade1", 1],
        ["grade1", None],
        [None    , 3] 
    ]
    datas   = [
        # class 1
        [
            # student
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004,1601481600005],
            ["Mary",       "Tom",        "Jack",       "Jane",       "alex"       ,None         ],
            [0,            1,            1,            0,            1            ,None         ],
            [98,           80,           60,           100,          99           ,None         ]
        ],
        # class 2
        [
            # student
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004,1601481600005],
            ["Mary2",      "Tom2",       "Jack2",       "Jane2",     "alex2"      ,None         ],
            [0,            1,            1,             0,           1            ,0            ],
            [298,          280,          260,           2100,        299          ,None         ]
        ],
        # class 3
        [
            # student
            [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004,1601481600005],
            ["Mary3",      "Tom3",       "Jack3",       "Jane3",     "alex3"       ,"Mark"      ],
            [0,            1,            1,             0,           1             ,None        ],
            [398,          380,          360,           3100,        399           ,None        ]
        ]
    ]

    stmt2.bind_param(tbanmes, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, stbname, tbanmes, tags, datas)



# insert with single table (performance is lower)
def insert_bind_param_with_tables(conn, stmt2, dbname, stbname):

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
                [1601481600000,1601481600004,"2024-09-19 10:00:00", "2024-09-19 10:00:01.123", datetime(2024,9,20,10,11,12,456)],
                ["Mary",       "Tom",        "Jack",                "Jane",                    "alex"       ],
                [0,            1,            1,                     0,                         1            ],
                [98,           80,           60,                    100,                       99           ]
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

    # bind with single table
    stmt2.bind_param_with_tables([table0, table1, table2])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, stbname, tbanmes, tags, datas)

# insert with single table (performance is lower)
def insert_with_normal_tables(conn, stmt2, dbname):

    tbanmes = ["ntb"]
    tags    = [None]
    # prepare data
    datas = [
            # table 1
            [
                # student
                [1601481600000,1601481600004,"2024-09-19 10:00:00", "2024-09-19 10:00:01.123", datetime(2024,9,20,10,11,12,456)],
                ["Mary",       "Tom",        "Jack",                "Jane",                    "alex"       ],
                [0,            3.14,         True,                     0,                         1            ],
                [98,           80,           60,                    100,                       99           ]
            ]
    ]

    table0 = BindTable(tbanmes[0], tags[0])
    for data in datas[0]:
       table0.add_col_data(data)

    # bind with single table
    stmt2.bind_param_with_tables([table0])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, None, tbanmes, tags, datas)


# insert except test
def insert_except_test(conn, stmt2):

    tbanmes = ["t1", "t2", "t3"]
    tags    = [
        ["grade2", 1],
        None,
        ["grade2", 3]
    ]

    # prepare data
    datas = [
            # table 1
            [
                # student
                [1601481600000,1601481600004,"2024-09-19 10:00:00", "2024-09-19 10:00:01.123", datetime(2024,9,20,10,11,12,456)],
                ["Mary",       "Tom",        "Jack",                "Jane",                    "alex"       ],
                [0,            1,            1,                     0,                         1            ],
                [98,           80,           60,                    100,                       99           ]
            ],
            None,
            None
    ]

    table0 = BindTable(tbanmes[0], tags[0])
    table1 = BindTable(tbanmes[1], tags[1])
    table2 = BindTable(tbanmes[2], tags[2])

    for data in datas[0]:
       table0.add_col_data(data)

    table1.add_col_data(datas[1])
    table2.add_col_data(datas[2])

    # bind with single table
    try:
        stmt2.bind_param_with_tables([table0, table1, table2])
    except Exception as err:
        print(f"check except is pass. err={err}")

#
# insert
#
def test_stmt2_insert(conn):
    if not IS_V3:
        print(" test_stmt2_query not support TDengine 2.X version.")
        return 

    dbname  = "stmt2"
    stbname = "meters"

    try:
        prepare(conn, dbname, stbname)
        # prepare
        stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?,?) values(?,?,?,?)")
        print("insert prepare sql ............................ ok\n")

        # insert with table
        insert_bind_param_with_tables(conn, stmt2, dbname, stbname)
        print("insert bind with tables ....................... ok\n")        

        # insert with split args
        insert_bind_param(conn, stmt2, dbname, stbname)
        print("insert bind ................................... ok\n")
        print("insert execute ................................ ok\n")
        stmt2.close()

        stmt2 = conn.statement2(f"insert into {dbname}.ntb values(?,?,?,?)")
        insert_with_normal_tables(conn, stmt2, dbname)
        print("insert normal tables .......................... ok\n")

        # insert except test
        insert_except_test(conn, stmt2)
        print("test insert except ............................ ok\n")           

        #conn.execute("drop database if exists %s" % dbname)
        stmt2.close()
        conn.close()
        print("test_stmt2_insert ............................. [passed]\n") 
    except Exception as err:
        #conn.execute("drop database if exists %s" % dbname)
        print("insert ........................................ failed\n")
        conn.close()
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
        log.debug(f" res2 rows = {row} \n")
        lres2.append(row)

    res1 = conn.query(sql2)
    for row in res1:
        log.debug(f" res1 rows = {row} \n")
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
def test_stmt2_query(conn):
    if not IS_V3:
        print(" test_stmt2_query not support TDengine 2.X version.")
        return 

    dbname  = "stmt2"
    stbname = "meters"
    sql1 = f"select * from {dbname}.d2 where name in (?) or score > ? ;"
    sql2 = f"select * from {dbname}.d2 where name in ('Tom2') or score > 1000;"

    try:
        # prepare
        prepare(conn, dbname, stbname)

        # prepare
        stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?,?) values(?,?,?,?)")
        insert_bind_param_with_tables(conn, stmt2, dbname, stbname)
        insert_bind_param(conn, stmt2, dbname, stbname)
        print("insert bind & execute ......................... ok\n")        

        
        # statement2
        stmt2 = conn.statement2(sql1)
        print("query prepare sql ............................. ok\n")


        # insert with table
        #insert_bind_param_with_tables(conn, stmt2)


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
        conn.close()
        print("test_stmt2_query .............................. [passed]\n") 

    except Exception as err:
        print("query ......................................... failed\n")
        conn.close()
        raise err


if __name__ == "__main__":
    print("start stmt2 test case...\n")
    taos.log.setting(True, True, True, True, True, False)

    # insert 
    test_stmt2_insert(taos.connect())

    # query
    test_stmt2_query(taos.connect())

    print("end stmt2 test case.\n")