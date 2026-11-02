# Copyright (c) 2023 TAOS Data, Inc.
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

# ANCHOR: connect

import taosws

conn = taosws.connect("taosws://root:taosdata@localhost:6041")
# ANCHOR_END: connect

# ANCHOR: basic
conn.execute("drop database if exists connwspy", req_id=1)
conn.execute("create database if not exists connwspy", req_id=2)
conn.execute("use connwspy", req_id=3)
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)", req_id=4)
conn.execute("create table if not exists tb1 using stb tags (1)", req_id=5)
conn.execute("insert into tb1 values (now, 1)", req_id=6)
conn.execute("insert into tb1 values (now, 2)", req_id=7)
conn.execute("insert into tb1 values (now, 3)", req_id=8)

r = conn.execute("select * from stb", req_id=9)
result = conn.query("select * from stb", req_id=10)
num_of_fields = result.field_count
print(num_of_fields)

for row in result:
    print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)
