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

import json
import taos
import requests

j = requests.get("http://api.coindesk.com/v1/bpi/currentprice.json").json()

# json to sql
ts = j["time"]["updatedISO"]
sql = "insert into"
for id in j["bpi"]:
    bpi = j["bpi"][id]
    sql += ' %s using price tags("%s","%s","%s") values("%s", %lf) ' % (
        id,
        bpi["code"],
        bpi["symbol"],
        bpi["description"],
        ts,
        bpi["rate_float"],
    )

# sql to TDengine
taos = taos.connect()
taos.execute("create database if not exists bpi")
taos.execute("use bpi")
taos.execute(
    "create stable if not exists price (ts timestamp, rate double)"
    + " tags (code binary(10), symbol binary(10), description binary(100))"
)
taos.execute(sql)
