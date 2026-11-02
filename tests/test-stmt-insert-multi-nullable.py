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

from taos import *
import taos
import pytest
import time
from random import random


@pytest.fixture
def conn():
    return taos.connect()


def test_stmt_insert_multi_nullable(conn):
    dbname = "pytest_taos_stmt_multi"
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

    params = new_multi_binds(16)
    params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
    params[1].bool((True, None, False))
    params[2].tinyint([-128, -128, None])  # -128 is tinyint null
    params[3].tinyint([0, 127, None])
    params[4].smallint([3, None, 2])
    params[5].int([3, 4, None])
    params[6].bigint([3, 4, None])
    params[7].tinyint_unsigned([3, 4, None])
    params[8].smallint_unsigned([3, 4, None])
    params[9].int_unsigned([3, 4, None])
    params[10].bigint_unsigned([3, 4, None])
    params[11].float([3, None, 1])
    params[12].double([3, None, 1.2])
    params[13].binary(["abc", "dddafadfadfadfadfa", None])
    # params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
    params[14].nchar([None, None, None])
    params[15].timestamp([None, None, 1626861392591])
    stmt.bind_param_batch(params)
    stmt.execute()

    assert stmt.affected_rows == 3

    result = conn.query("select * from log")
    for row in result:
        print(row)
    result.close()
    stmt.close()
    conn.close()
