import taosrest


def _test_connect():
    conn = taosrest.connect(host="192.168.1.163",
                            port=8085,
                            username="root",
                            password="taosdata",
                            token="c37ef4dbec8708c0227b4e8cb84ffffb9b8711a1")

    print(conn.server_info)


def _test_query():
    conn = taosrest.connect(host="192.168.1.163",
                            port=8085,
                            username="root",
                            password="taosdata",
                            token="c37ef4dbec8708c0227b4e8cb84ffffb9b8711a1")
    cursor = conn.cursor()
    cursor.execute("drop database if exists pytest")
    cursor.execute("create database pytest precision 'ns' keep 365")
    cursor.execute("create table pytest.temperature(ts timestamp, temp int)")
    cursor.execute("insert into pytest.temperature values(now, 1) (now+10b, 2)")
    cursor.execute("select * from pytest.temperature")
    rows = cursor.fetchall()
    print(rows)
