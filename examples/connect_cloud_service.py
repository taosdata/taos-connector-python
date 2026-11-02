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

import taosrest
import os


def connect_to_cloud_use_token(url, token):
    conn = taosrest.connect(url=url, token=token)

    print(conn.server_info)
    cursor = conn.cursor()
    cursor.execute("drop database if exists pytest")
    cursor.execute("create database pytest precision 'ns' keep 365")
    cursor.execute("create table pytest.temperature(ts timestamp, temp int)")
    cursor.execute("insert into pytest.temperature values(now, 1) (now+10b, 2)")
    cursor.execute("select * from pytest.temperature")
    rows = cursor.fetchall()
    print(rows)


if __name__ == "__main__":
    url = os.environ["TDENGINE_CLOUD_URL"]
    token = os.environ["TDENGINE_CLOUD_TOKEN"]
    connect_to_cloud_use_token(url, token)
