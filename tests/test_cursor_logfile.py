from os import unlink
import taos


def test_logfile():
    conn = taos.connect()
    cursor = conn.cursor()

    try:
      unlink("log.txt")
    except:
      None
    cursor.log("log.txt")
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

    logs = open("log.txt", encoding="utf-8")
    txt = logs.read().splitlines()
    assert txt == [
        "DROP DATABASE IF EXISTS test;",
        "CREATE DATABASE test;",
        "USE test;",
        "CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT);",
        "INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+100a, 23.5);",
        "SELECT tbname, ts, temperature, location FROM weather LIMIT 1;",
    ]
    unlink("log.txt")
