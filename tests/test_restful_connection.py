import taosrest


def test_fetch_all():
    conn = taosrest.connect(host="localhost",
                            user="root",
                            password="taosdata",
                            database="test",
                            prot=6041)
    cursor = conn.cursor()

    cursor.execute("show databases")
    results: list[tuple] = cursor.fetchall()
    for row in results:
        print(row)
    print(cursor.description)


def test_fetch_one():
    conn = taosrest.connect(host="localhost",
                            user="root",
                            password="taosdata",
                            database="test",
                            prot=6041)
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.executemany("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    c.execute("select * from test.tb")
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()
