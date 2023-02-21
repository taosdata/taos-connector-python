import multiprocessing
import time
from datetime import datetime

import taos

from multiprocessing import pool


def test_cursor():
    conn = taos.connect(host='192.168.1.95', port=6030)
    cursor = conn.cursor()
    while True:
        cursor.execute("select count(*) from test_query_py.tb1 limit 1")
        print(cursor.fetchall())
        time.sleep(1)
        conn.execute(
            "insert into test_query_py.tn1 using test_query_py.tb1 tags('{\"name\":\"value\"}') "
            "values(now, null) (now + 10s, 1)")


def test_query():
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    start = datetime.now()

    multiprocessing.set_start_method('spawn')
    p = pool.Pool(10)

    async_result = []
    for _ in range(10):
        async_result.append(p.apply_async(func=test_cursor))

    for r in async_result:
        r.get()

    p.close()
    p.join()

    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)


if __name__ == "__main__":
    test_query()
