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

import taosws

conn = taosws.connect("ws://localhost:6041")

conn.execute("create database if not exists connwspy")
conn.execute("use connwspy")
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute("create table if not exists tb1 using stb tags (1)")
conn.execute("insert into tb1 values (now, 1)")
conn.execute("insert into tb1 values (now, 2)")
conn.execute("insert into tb1 values (now, 3)")

result = conn.query("show databases")
num_of_fields = result.field_count
print(num_of_fields)

for field in result.fields:
    print(field)
for row in result:
    print(row)

conn.execute("drop database if exists connwspy")
