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

# encoding:UTF-8

import taos

conn = taos.connect()
conn.execute("drop database if exists py_test_json_type")
conn.execute("create database if not exists py_test_json_type")
conn.execute("use py_test_json_type")

conn.execute("create stable s1 (ts timestamp, v1 int) tags (info json)")
conn.execute('create table s1_1 using s1 tags (\'{"k1": "v1"}\')')
tags = conn.query("select info, tbname from s1").fetch_all_into_dict()
print(tags)

k1 = conn.query("select info->'k1' as k1 from s1").fetch_all_into_dict()
print(k1)

conn.execute("drop database py_test_json_type")

conn.close()
