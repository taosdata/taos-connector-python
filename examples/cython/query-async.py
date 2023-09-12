import asyncio
import time
from taos._objects import TaosConnection


def print_all(fut: asyncio.Future):
    result = fut.result()
    print("result:", result)
    print("fields:", result.fields)
    # print("data:", result.fetch_all()) # NOT USE fetch_all directly right here

def callback_print_row(conn, cb=None):
    query_task = asyncio.create_task(conn.query_a("select * from power.meters"))
    if cb:
        query_task.add_done_callback(cb)

    return query_task

async def test_query(conn):
    conn.execute('create database if not exists power')
    conn.execute(
        'CREATE STABLE if not exists power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) '
        'TAGS (location BINARY(64), groupId INT)')

    # async iter row
    result = await conn.query_a("select * from power.meters")
    print("result:", result)
    async for row in result:
        print("row:", row)

    
    # callback and async fetch all data
    task = callback_print_row(conn, print_all)
    res = await task
    data = await res.fetch_all_a()
    print(data)


if __name__ == "__main__":
    conn = TaosConnection(host="localhost")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_query(conn))
    conn.close()
