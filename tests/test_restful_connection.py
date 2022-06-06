import taosrest


def test_fetch_all():
    conn = taosrest.connect(url="http://localhost:6041",
                            password="taosdata",
                            database="test",
                            port=6041)
    cursor = conn.cursor()

    cursor.execute("show databases")
    results: list[tuple] = cursor.fetchall()
    for row in results:
        print(row)
    print(cursor.description)


def test_fetch_one():
    conn = taosrest.connect(url="localhost:6041",
                            user="root",
                            password="taosdata",
                            database="test",
                            port=6041)
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.executemany("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    assert c.rowcount == 2
    c.execute("select * from test.tb")
    print()
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()


def test_row_count():
    conn = taosrest.connect(url="localhost:6041", user="root", password="taosdata")
    cursor = conn.cursor()
    cursor.execute("select * from test.tb")
    assert cursor.rowcount == 2


def test_get_server_info():
    conn = taosrest.connect(host="localhost:6041",
                            user="root",
                            password="taosdata",
                            database="test")

    version: str = conn.server_info
    assert len(version.split(".")) == 4
