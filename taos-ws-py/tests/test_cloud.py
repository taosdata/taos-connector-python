# Copyright (c) 2024 TAOS Data, Inc.
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

import sys
import taosws


def taos_ws_connect(token):
    print("-" * 40)
    print("test_ws_connect")
    conn = taosws.connect("%s?token=%s" % ("wss://gw.cloud.taosdata.com", token))
    res = conn.query_with_req_id("show databases", 1)
    dbs = [row[0] for row in res]
    print(dbs)
    # check sys db exist
    assert "information_schema" in dbs
    assert "performance_schema" in dbs
    print("test_ws_connect done")
    print("-" * 40)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("need token argument passed.\n")
    else:
        taos_ws_connect(sys.argv[1])
