from datetime import datetime
import taos
import pytest

@pytest.fixture
def conn():
    return taos.connect()

def test_query(conn):
    # type: (taos.TaosConnection) -> None
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    conn.execute("drop database if exists test_query_py")
    conn.execute("create database if not exists test_query_py")
    conn.execute("use test_query_py")
    conn.execute("create table if not exists tb1 (ts timestamp, v int) tags(jt json)")
    n = conn.execute("insert into tn1 using tb1 tags('{\"name\":\"value\"}') values(now, null) (now + 10s, 1)")
    n = conn.execute("insert into tn1 using tb1 tags('{\"name\":\"value\"}') values(now, null) (now + 10s, 1)")
    print("inserted %d rows" % n)
    result = conn.query("select * from tb1")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)
    start = datetime.now()
    for row in result:
        print(row)
        None
    result = conn.query("select * from tb1 limit 1")
    results = result.fetch_all_into_dict()
    print(results)

    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    conn.close()

def _test_query_row_iter(conn):
    """This test will use fetch_row for each row fetching, this is the only way in async callback"""
    result = conn.query("select * from log.log limit 10000")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)
    start = datetime.now()
    for row in result.rows_iter():
        # print(row)
        None
    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    conn.close()

if __name__ == "__main__":
    conn1 = taos.connect()
    test_query(conn1)
