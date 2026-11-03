# Copyright (c) 2021 TAOS Data, Inc.
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

import taos
import random

conn = taos.connect()
dbname = "pytest_taos_subscribe"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)
conn.execute("create table if not exists log(ts timestamp, n int)")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % i)

sub = conn.subscribe(False, "test", "select * from log", 1000)
print("# consume from begin")
for ts, n in sub.consume():
    print(ts, n)

print("# consume new data")
for i in range(5):
    conn.execute("insert into log values(now, %d)(now+1s, %d)" % (i, i))
    result = sub.consume()
    for ts, n in result:
        print(ts, n)

sub.close(True)
print("# keep progress consume")
sub = conn.subscribe(False, "test", "select * from log", 1000)
result = sub.consume()
rows = result.fetch_all()
# consume from latest subscription needs root privilege(for /var/lib/taos).
assert result.row_count == 0
print("## consumed ", len(rows), "rows")

print("# consume with a stop condition")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % random.randint(0, 10))
    result = sub.consume()
    try:
        ts, n = next(result)
        print(ts, n)
        if n > 5:
            result.stop_query()
            print("## stopped")
            break
    except StopIteration:
        continue

sub.close()

# sub.close()

conn.execute("drop database if exists %s" % dbname)
# conn.close()
