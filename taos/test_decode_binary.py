#!
import taos


def test_decode_binary():
    conn = taos.connect(decode_binary=False)
    conn.execute("drop database if exists test_decode_binary")
    conn.execute("create database if not exists test_decode_binary")
    conn.execute("use test_decode_binary")

    conn.execute("create table test_decode_binary (ts timestamp, c1 nchar(10), c2 binary(50))")
    conn.execute("insert into test_decode_binary values ('2020-01-01 00:00:00', 'hello', 'world')")
    conn.execute(
        "insert into test_decode_binary values ('2020-01-01 00:00:01', 'hello', '\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09')")

    result = conn.query("select * from test_decode_binary")
    print(result.fetch_all())
    for row in result:
        print(row)

    conn.execute("drop database if exists test_decode_binary")
