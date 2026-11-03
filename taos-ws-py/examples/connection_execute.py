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
import datetime

env = {
    "db_protocol": "taosws",
    "db_user": "root",
    "db_pass": "taosdata",
    "db_host": "localhost",
    "db_port": 6041,
    "db_name": "t_ws",
}


def make_context(config):
    db_protocol = config["db_protocol"]
    db_user = config["db_user"]
    db_pass = config["db_pass"]
    db_host = config["db_host"]
    db_port = config["db_port"]

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"

    db_name = config["db_name"]

    conn = taosws.connect(db_url)

    return conn, db_name


def execute():
    conn, db = make_context(env)
    res = conn.execute("show dnodes")
    print(f"res: {res}")
    conn.execute(f"drop database if exists {db}")
    conn.execute(f"create database {db}")
    conn.execute(f"use {db}")
    conn.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    data = [
        {
            "name": "tb1",
            "t1": 1,
        },
        {
            "name": "tb2",
            "t1": 2,
        },
        {
            "name": "tb3",
            "t1": 3,
        },
    ]

    for d in data:
        res = conn.execute(
            f"create table {d.get('name')} using stb tags({d.get('t1')})",
        )
        print(f"res: {res}")

    ts = datetime.datetime.now().astimezone()
    data = [
        ("tb1", ts, 1),
        ("tb2", ts, 2),
        ("tb3", ts, 3),
    ]

    for d in data:
        res = conn.execute(
            f"insert into {d[0]} values('{d[1]}', {d[2]})",
        )
        print(f"res: {res}")

    row = conn.execute("select * from stb")
    print(f"row: {row}")
    conn.close()


if __name__ == "__main__":
    execute()
