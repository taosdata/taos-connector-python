# Copyright (c) 2025 TAOS Data, Inc.
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

import taosws
import time


def test_fetchmany():
    try:
        conn = taosws.connect("taosws://localhost:6041")
        conn.execute("drop database if exists test_173942943")
        conn.execute("create database test_173942943")
        conn.execute("use test_173942943")
        conn.execute("create table t0 (ts timestamp, c1 int)")

        num = 7000
        ts = time.time_ns() // 1000000
        for i in range(num):
            conn.execute(f"insert into t0 values({ts+i}, {i})")

        cursor = conn.cursor()
        cursor.execute("select * from test_173942943.t0")

        rows = 0
        while True:
            res = cursor.fetchmany(3345)
            if res is None or len(res) == 0:
                break
            else:
                rows += len(res)

        assert rows == num

        conn.execute("drop database test_173942943")
    except Exception as err:
        print(f"fetchmany failed, err: {err}")
        raise err
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    test_fetchmany()
