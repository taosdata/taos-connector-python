import pytest
import taosws
from os import unlink

config = [
    {
        'db_protocol': 'taosws',
        'db_user': "root",
        'db_pass': "taosdata",
        'db_host': "td-1",
        'db_port': 6041,
        'db_name': "test",
    }
]


@pytest.fixture(params=config)
def ctx(request):
    db_protocol = request.param['db_protocol']
    db_user = request.param['db_user']
    db_pass = request.param['db_pass']
    db_host = request.param['db_host']
    db_port = request.param['db_port']

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"

    db_name = request.param['db_name']

    conn = taosws.connect(db_url)
    yield conn, db_name
    conn.execute_with_req_id("DROP DATABASE IF EXISTS %s" % db_name)
    conn.close()


def test_logfile(ctx):
    conn, db = ctx
    cursor = conn.cursor()

    try:
        unlink("log.txt")
    except FileNotFoundError:
        pass
    cursor.log("log.txt")
    cursor.execute_with_req_id("DROP DATABASE IF EXISTS test")
    cursor.execute_with_req_id("CREATE DATABASE test")
    cursor.execute_with_req_id("USE test")
    cursor.execute_with_req_id("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
    cursor.execute_with_req_id(f"INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+100a, 23.5)")
    assert cursor.rowcount == 2
    cursor.execute_with_req_id("SELECT tbname, ts, temperature, location FROM weather LIMIT 1")
    # rowcount can only get correct value after fetching all data
    all_data = cursor.fetchall()
    assert cursor.rowcount == 1
    cursor.close()

    logs = open("log.txt", encoding="utf-8")
    txt = logs.read().splitlines()
    assert txt == [
        "DROP DATABASE IF EXISTS test;",
        "CREATE DATABASE test;",
        "USE test;",
        "CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT);",
        "INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+100a, 23.5);",
        "SELECT tbname, ts, temperature, location FROM weather LIMIT 1;",
        "DROP DATABASE IF EXISTS test;",
    ]
    unlink("log.txt")
