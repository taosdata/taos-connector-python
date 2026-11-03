#! encoding=utf-8

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

import taos
from utils import tear_down_database


def test_get_table_vgroup_id():
    conn = taos.connect()
    res = conn.query("select server_version()")
    data = res.fetch_all()
    res.close()
    if data[0][0] < "3.0.2.0":
        return

    conn.execute("drop database if exists test_get_table_vgroup_id")
    conn.execute("create database if not exists test_get_table_vgroup_id")
    conn.execute(
        "create stable test_get_table_vgroup_id.meters (ts timestamp, current float, voltage int, phase float)"
        " tags (location binary(64), groupId int)"
    )
    conn.execute(
        "create table test_get_table_vgroup_id.d0 using test_get_table_vgroup_id.meters "
        "tags ('California.SanFrancisco', 1)"
    )
    conn.execute(
        "create table test_get_table_vgroup_id.d1 using test_get_table_vgroup_id.meters "
        "tags ('California.LosAngles', 2)"
    )

    vg_id = conn.get_table_vgroup_id("test_get_table_vgroup_id", "d0")
    print(vg_id)
    print("\n")
    vg_id = conn.get_table_vgroup_id("test_get_table_vgroup_id", "d1")
    print(vg_id)
    print("\n")
    db_name = "test_get_table_vgroup_id"
    tear_down_database(conn, db_name)
    conn.close()


if __name__ == "__main__":
    test_get_table_vgroup_id()
