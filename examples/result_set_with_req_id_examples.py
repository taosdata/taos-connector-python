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

import taos

conn = taos.connect()
conn.execute("DROP DATABASE IF EXISTS test", req_id=1)
conn.execute("CREATE DATABASE test", req_id=2)
conn.select_db("test")
conn.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)", req_id=3)
# prepare data
for i in range(2000):
    location = str(i % 10)
    tb = "t" + location
    conn.execute(
        f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)", req_id=4 + i
    )

result: taos.TaosResult = conn.query("SELECT * FROM weather", req_id=2004)

block_index = 0
blocks: taos.TaosBlocks = result.blocks_iter()
for rows, length in blocks:
    print("block ", block_index, " length", length)
    print("first row in this block:", rows[0])
    block_index += 1

conn.close()

# possible output:
# block  0  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 46000), 23.5, 0)
# block  1  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 76000), 23.5, 3)
# block  2  length 1200
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 99000), 23.5, 6)
# block  3  length 400
# first row in this block: (datetime.datetime(2022, 4, 27, 15, 14, 52, 122000), 23.5, 9)
