import taos


def test_cursor():
    conn = taos.connect()
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test")
    cursor.execute("CREATE DATABASE test")
    cursor.execute("USE test")
    cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")
    cursor.execute(f"INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+100a, 23.5)")
    assert cursor.rowcount == 2
    cursor.execute("SELECT tbname, ts, temperature, location FROM weather LIMIT 1")
    # rowcount can only get correct value after fetching all data
    all_data = cursor.fetchall()
    assert cursor.rowcount == 1
    cursor.close()
    conn.close()
