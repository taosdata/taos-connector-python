import gc
import os
import time
from ctypes import CFUNCTYPE, POINTER, Structure, byref, c_bool, c_int, c_void_p, cast

import taos
from taos import TaosResult

ROWS = int(os.environ.get("ASYNC_CRASH_ROWS", "500"))
ITERS = int(os.environ.get("ASYNC_CRASH_ITERS", "15"))


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]


def fetch_callback(p_param, p_result, num_of_rows):
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)
    if num_of_rows <= 0:
        p.contents.done = True
        result.close()
        return
    for _ in result.rows_iter(num_of_rows):
        pass
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)


def query_callback(p_param, p_result, code):
    if p_result is None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)


def churn():
    junk = [CFUNCTYPE(None, c_void_p, c_void_p, c_int)(lambda a, b, c: None) for _ in range(200)]
    del junk
    junk = [bytearray(4096) for _ in range(200)]
    del junk


def main():
    conn = taos.connect()
    conn.execute("drop database if exists test_1783663885")
    conn.execute("create database test_1783663885")
    conn.execute("use test_1783663885")
    conn.execute("create table t0 (ts timestamp, c1 int)")

    base = 1700000000000
    for i in range(0, ROWS, 100):
        vals = ",".join("(%d,%d)" % (base + i + j, i + j) for j in range(min(100, ROWS - i)))
        conn.execute("insert into t0 values %s" % vals)

    for _ in range(ITERS):
        counter = Counter(count=0)
        conn.query_a("select * from t0", query_callback, byref(counter))
        gc.collect()
        churn()
        deadline = time.time() + 5
        while not counter.done and time.time() < deadline:
            gc.collect()
            churn()
            time.sleep(0.001)
        assert counter.done, "async callback did not complete within timeout"

    conn.execute("drop database if exists test_1783663885")
    conn.close()
    print("SURVIVED")


if __name__ == "__main__":
    main()
