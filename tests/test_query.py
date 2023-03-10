from datetime import datetime

from utils import tear_down_database
from taos import utils
from taos.error import InterfaceError
import taos


def test_query():
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    conn = taos.connect()
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

    # test re-consume fields
    flag = 0
    for _ in fields:
        flag += 1
    assert flag == 3

    start = datetime.now()
    for row in result:
        print(row)
        None

    for row in result.rows_iter():
        print(row)

    result = conn.query("select * from tb1 limit 1")
    results = result.fetch_all_into_dict()
    print(results)

    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    db_name = "test_query_py"
    tear_down_database(conn, db_name)
    conn.close()


def test_query_with_req_id():
    db_name = "test_query_py"
    conn = taos.connect()
    try:
        conn.execute("drop database if exists test_query_py")
        conn.execute("create database if not exists test_query_py")
        conn.execute("use test_query_py")
        conn.execute("create table if not exists tb1 (ts timestamp, v int) tags(jt json)")
        n = conn.execute("insert into tn1 using tb1 tags('{\"name\":\"value\"}') values(now, null) (now + 10s, 1)")
        n = conn.execute("insert into tn1 using tb1 tags('{\"name\":\"value\"}') values(now, null) (now + 10s, 1)")
        print("inserted %d rows" % n)
        req_id = utils.gen_req_id()
        result = conn.query("select * from tb1", req_id)
        fields = result.fields
        for field in fields:
            print("field: %s" % field)

        # test re-consume fields
        flag = 0
        for _ in fields:
            flag += 1
        assert flag == 3

        start = datetime.now()
        for row in result:
            print(row)
            None

        for row in result.rows_iter():
            print(row)

        req_id = utils.gen_req_id()
        result = conn.query("select * from tb1", req_id)
        results = result.fetch_all_into_dict()
        print(results)

        end = datetime.now()
        elapsed = end - start
        print("elapsed time: ", elapsed)
        result.close()
    except InterfaceError as e:
        print(e)
    except Exception as e:
        print(e)
        tear_down_database(conn, db_name)
        conn.close()
        raise e
    finally:
        tear_down_database(conn, db_name)
        conn.close()


if __name__ == "__main__":
    test_query()
