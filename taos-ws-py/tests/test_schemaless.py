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

import taosws
from taosws import PySchemalessProtocol, PySchemalessPrecision

line_data = [
    "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
    "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
    "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
    "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
]

json_data = [
    """[
    {"metric": "meters.current", "timestamp": 1681345954000, "value": 10.3, 
    "tags": {"location": "California.SanFrancisco", "groupid": 2}}, 
    {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, 
    "tags": {"location": "California.SanFrancisco", "groupid": 2}}
    ]"""
]

telnet_data = [
    "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2",
    "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2",
    "meters.voltage 1648432611249 221 location=California.LosAngeles group=3",
    "meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
]


def test_schemaless():
    conn = taosws.connect()

    conn.execute("drop database if exists test_sml")
    conn.execute("create database if not exists test_sml")
    conn.execute("use test_sml")

    conn.schemaless_insert(
        lines=line_data,
        protocol=PySchemalessProtocol.Line,
        precision=PySchemalessPrecision.Millisecond,
        ttl=0,
        req_id=123,
    )

    conn.schemaless_insert(
        lines=json_data,
        protocol=PySchemalessProtocol.Json,
        precision=PySchemalessPrecision.Millisecond,
        ttl=0,
        req_id=123,
    )

    conn.schemaless_insert(
        lines=telnet_data,
        protocol=PySchemalessProtocol.Telnet,
        precision=PySchemalessPrecision.Millisecond,
        ttl=0,
        req_id=123,
    )

    res = conn.query("select count(*) from test_sml.measurement")
    assert all(row[0] == 4 for row in res)

    conn.execute("drop database if exists test_sml")
    conn.close()


if __name__ == "__main__":
    test_schemaless()
