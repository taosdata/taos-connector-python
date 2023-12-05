import asyncio
import time
from taos._objects import TaosConnection
from taos import utils
import pytest

pytest_plugins = ('pytest_asyncio',)


@pytest.fixture
def conn():
    return TaosConnection(host="localhost")

def print_all(fut: asyncio.Future):
    result = fut.result()
    print("result:", result)
    print("fields:", result.fields)
    # print("data:", result.fetch_all()) # NOT USE fetch_all directly right here

@pytest.mark.asyncio
async def test_query_a(conn):
    conn.execute("drop database if exists pytestquerya")
    conn.execute("create database pytestquerya")
    conn.execute("use pytestquerya")
    cols = ["bool", "tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned",
            "bigint unsigned", "float", "double", "binary(100)", "nchar(100)"]
    s = ','.join("c%d %s" % (i, t) for i, t in enumerate(cols))
    print(s)
    conn.execute("create table tb1(ts timestamp, %s)" % s)
    for _ in range(100):
        s = ','.join('null' for c in cols)
        conn.execute("insert into tb1 values(now, %s)" % s)

    # async query, async iter row
    result = await conn.query_a("select * from tb1")
    print("result:", result)
    async for row in result:
        print("row:", row)

    # async query, add callback, async fetch all data
    query_task = asyncio.create_task(conn.query_a("select * from tb1"))
    query_task.add_done_callback(print_all)
    res = await query_task
    data = await res.fetch_all_a()
    print(data)

    # async query with req_id, async iter row
    result = await conn.query_a("select * from tb1", req_id=utils.gen_req_id())
    print("result:", result)
    async for row in result:
        print("row:", row)

    
    # async query with req_id, add callback, async fetch all data
    query_task = asyncio.create_task(conn.query_a("select * from tb1", req_id=utils.gen_req_id()))
    query_task.add_done_callback(print_all)
    res = await query_task
    data = await res.fetch_all_a()
    print(data)

    conn.execute("drop database if exists pytestquerya")
    conn.close()

