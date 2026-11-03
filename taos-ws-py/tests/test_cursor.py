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
import datetime

conn = taosws.connect()
cursor = conn.cursor()

cursor.execute("select 1")
db = "t_ws"
cursor.execute("drop database if exists {}", db)
cursor.execute("create database {}", db)
cursor.execute("use {name}", name=db)
cursor.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

data = [{"name": "tb1", "t1": 1}, {"name": "tb2", "t1": 2}]
cursor.execute_many("create table {name} using stb tags({t1})", data)

ts = datetime.datetime.now().astimezone()
data = [("tb1", ts, 1), ("tb2", ts, 2)]
cursor.execute_many("insert into {} values('{}', {})", data)
