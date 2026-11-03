# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from os import unlink
from utils import tear_down_database
import taos


def test_logfile():
    conn = taos.connect()
    cursor = conn.cursor()

    try:
        unlink("log.txt")
    except Exception:
        pass
    cursor.log("log.txt")
    cursor.execute("DROP DATABASE IF EXISTS test")
    cursor.execute("CREATE DATABASE test")
    cursor.execute("USE test")
    cursor.execute(
        "CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT, city NCHAR(100), country BINARY(100), town VARBINARY(100)) TAGS (location INT)"
    )
    cursor.execute(
        f"INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5, 'tianjin', 'china', 'wuqing') (now+100a, 23.5, 'tianjin', 'china', 'wuqing')"
    )
    assert cursor.rowcount == 2
    cursor.execute("SELECT tbname, ts, temperature, city, country, town, location FROM weather LIMIT 1")
    # rowcount can only get correct value after fetching all data
    all_data = cursor.fetchall()
    assert cursor.rowcount == 1
    db_name = "test"
    tear_down_database(cursor, db_name)
    cursor.close()
    conn.close()

    logs = open("log.txt", encoding="utf-8")
    txt = logs.read().splitlines()
    assert txt == [
        "DROP DATABASE IF EXISTS test;",
        "CREATE DATABASE test;",
        "USE test;",
        "CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT, city NCHAR(100), country BINARY(100), town VARBINARY(100)) TAGS (location INT);",
        "INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5, 'tianjin', 'china', 'wuqing') (now+100a, 23.5, 'tianjin', 'china', 'wuqing');",
        "SELECT tbname, ts, temperature, city, country, town, location FROM weather LIMIT 1;",
        "DROP DATABASE IF EXISTS test;",
    ]
    unlink("log.txt")
