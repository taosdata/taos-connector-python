from datetime import datetime

from utils import tear_down_database
from taos import utils, IS_V3
from taos.error import InterfaceError
import taos


def test_query():
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    conn = taos.connect()
    conn.execute("drop database if exists test_query_py")
    conn.execute("create database if not exists test_query_py")
    conn.execute("use test_query_py")
    conn.execute("create table if not exists tb1 (ts timestamp, v blob) tags(jt json)")
    # n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null) (now + 10s, "xxxxxxxxxxxxxxxxxxx") (now + 20s, "\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09")')
    n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now + 10s, "xxxxxxxxxxxxxxxxxxx")')
    print("inserted %d rows" % n)
    result = conn.query("select * from tb1")
    fields = result.fields
    print("fields: ", fields)
    assert fields.count == 3

    results= result.fetch_all()
    assert results[0][1] == b"xxxxxxxxxxxxxxxxxxx"
    # assert results[0][1] == None
    # assert results[1][1] == b"xxxxxxxxxxxxxxxxxxx"
    # assert results[2][1] == b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
    assert len(results) == 1
    
    result.close()
    db_name = "test_query_py"
    tear_down_database(conn, db_name)
    conn.close()

if __name__ == "__main__":
    test_query()
