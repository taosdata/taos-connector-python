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

from taos import *

conn = connect()

dbname = "pytest_taos_stmt"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)

conn.execute(
    "create table if not exists log(ts timestamp, bo bool, nil tinyint, \
        ti tinyint, si smallint, ii int, bi bigint, tu tinyint unsigned, \
        su smallint unsigned, iu int unsigned, bu bigint unsigned, \
        ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
)

stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

params = new_bind_params(16)
params[0].timestamp(1626861392589)
params[1].bool(True)
params[2].tinyint(None)
params[3].tinyint(2)
params[4].smallint(3)
params[5].int(4)
params[6].bigint(5)
params[7].tinyint_unsigned(6)
params[8].smallint_unsigned(7)
params[9].int_unsigned(8)
params[10].bigint_unsigned(9)
params[11].float(10.1)
params[12].double(10.11)
params[13].binary("hello")
params[14].nchar("stmt")
params[15].timestamp(1626861392589)
stmt.bind_param(params)

params[0].timestamp(1626861392590)
params[15].timestamp(None)
stmt.bind_param(params)
stmt.execute()

assert stmt.affected_rows == 2
# No need to explicitly close, but ok for you
# result.close()

result = conn.query("select * from log")

for row in result:
    print(row)

# No need to explicitly close, but ok for you
result.close()
stmt.close()
conn.close()
