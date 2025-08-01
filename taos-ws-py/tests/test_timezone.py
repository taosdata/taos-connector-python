import taosws


# TODO: modify ip
# TODO: modify main
def test_cursor_timezone_default():
    conn = taosws.connect("ws://localhost:6041")
    cursor = conn.cursor()

    try:
        cursor.execute("drop database if exists test_1753948987")
        cursor.execute("create database test_1753948987")
        cursor.execute("use test_1753948987")
        cursor.execute("create table t0 (ts timestamp, c1 int)")
        cursor.execute("insert into t0 values ('2025-01-01 12:00:00', 1)")
        cursor.execute("insert into t0 values ('2025-01-02 15:30:00', 2)"),

        cursor.execute("select * from t0")
        rows = cursor.fetchall()

        assert len(rows) == 2

        fmt = "%Y-%m-%d %H:%M:%S %z"

        assert rows[0][0].strftime(fmt) == "2025-01-01 12:00:00 +0800"
        assert rows[1][0].strftime(fmt) == "2025-01-02 15:30:00 +0800"

        assert rows[0][1] == 1
        assert rows[1][1] == 2

    finally:
        cursor.execute("drop database test_1753948987")
        conn.close()


def test_cursor_timezone_custom():
    conn = taosws.connect("ws://localhost:6041?timezone=America/New_York")
    cursor = conn.cursor()

    try:
        cursor.execute("drop database if exists test_1753952584")
        cursor.execute("create database test_1753952584")
        cursor.execute("use test_1753952584")
        cursor.execute("create table t0 (ts timestamp, c1 int)")
        cursor.execute("insert into t0 values ('2025-01-01 12:00:00', 1)")
        cursor.execute("insert into t0 values ('2025-01-02 15:30:00', 2)"),

        cursor.execute("select * from t0")
        rows = cursor.fetchall()

        assert len(rows) == 2

        fmt = "%Y-%m-%d %H:%M:%S %z"

        assert rows[0][0].strftime(fmt) == "2025-01-01 12:00:00 -0500"
        assert rows[1][0].strftime(fmt) == "2025-01-02 15:30:00 -0500"

        assert rows[0][1] == 1
        assert rows[1][1] == 2

    finally:
        cursor.execute("drop database test_1753952584")
        conn.close()


def test_query_timezone_default():
    conn = taosws.connect("ws://localhost:6041")

    try:
        conn.execute("drop database if exists test_1754026484")
        conn.execute("create database test_1754026484")
        conn.execute("use test_1754026484")
        conn.execute("create table t0 (ts timestamp, c1 int)")
        conn.execute("insert into t0 values ('2025-01-01 12:00:00', 1)")
        conn.execute("insert into t0 values ('2025-01-02 15:30:00', 2)"),

        result = conn.query("select * from t0")

        expect_results = [("2025-01-01 12:00:00 +08:00", 1), ("2025-01-02 15:30:00 +08:00", 2)]

        actual_results = []
        for row in result:
            actual_results.append(row)

        assert actual_results == expect_results

    finally:
        conn.execute("drop database test_1754026484")
        conn.close()


def test_query_timezone_custom():
    conn = taosws.connect(
        host="localhost",
        port=6041,
        timezone="America/New_York",
    )

    try:
        conn.execute("drop database if exists test_1754027465")
        conn.execute("create database test_1754027465")
        conn.execute("use test_1754027465")
        conn.execute("create table t0 (ts timestamp, c1 int)")
        conn.execute("insert into t0 values ('2025-01-01 12:00:00', 1)")
        conn.execute("insert into t0 values ('2025-01-02 15:30:00', 2)"),

        result = conn.query("select * from t0")

        expect_results = [("2025-01-01 12:00:00 EST", 1), ("2025-01-02 15:30:00 EST", 2)]

        actual_results = []
        for row in result:
            actual_results.append(row)

        assert actual_results == expect_results

    finally:
        conn.execute("drop database test_1754027465")
        conn.close()
