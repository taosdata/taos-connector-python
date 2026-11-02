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
import os
from urllib.request import urlopen, Request

import pytest
from dotenv import load_dotenv

from decorators import check_env

load_dotenv()

default_token = "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"


@check_env
def test_login():
    url = os.environ["TDENGINE_URL"]
    response = urlopen(f"http://{url}/rest/login/root/taosdata")
    resp = json.load(response)
    print(resp)
    assert "code" in resp and resp["code"] == 0
    assert "desc" in resp and resp["desc"] == default_token


@check_env
def test_wrong_password():
    url = os.environ["TDENGINE_URL"]
    try:
        urlopen(f"http://{url}/rest/login/root/taosdata")
    except Exception as e:
        print(f"error: <{e}>")
        assert "HTTP Error 401: Unauthorized" in str(e), "wrong password should return Unauthorized"
    return


@check_env
def test_server_version():
    url = "http://localhost:6041/rest/sql"
    data = "select server_version()".encode("ascii")

    headers = {"Authorization": "Taosd " + default_token}
    request = Request(url, data, headers)
    response = urlopen(request)
    resp = json.load(response)
    print(resp)
    assert resp["rows"] == 1
    assert len(resp["column_meta"]) == 1
    assert len(resp["data"]) == 1


@check_env
def test_wrong_sql():
    """
    {'code': 9730, 'desc': 'Table does not exist: notable'}
    """
    url = "http://localhost:6041/rest/sql"
    data = "select * from nodb.notable".encode("utf8")

    headers = {"Authorization": "Taosd " + default_token}
    request = Request(url, data, headers)
    response = urlopen(request)
    resp = json.load(response)
    print("\n", resp)
    assert "code" in resp and resp["code"] != 0
