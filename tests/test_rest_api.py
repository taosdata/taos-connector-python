import json
import os
from urllib.request import urlopen, Request

import pytest
from dotenv import load_dotenv

from decorators import check_env

load_dotenv()

default_token = "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"


def test_login():
    response = urlopen("http://localhost:6041/rest/login/root/taosdata")
    resp = json.load(response)
    print()
    print(resp)
    assert "code" in resp and resp["code"] == 0
    assert "desc" in resp and resp["desc"] == default_token


def test_wrong_password():
    response = urlopen("http://localhost:6041/rest/login/root/taosdatax")
    resp = json.load(response)
    assert resp["code"] == 3
    assert resp["desc"] == "Authentication failure"


@pytest.mark.skip(reason="know bug TD-16959")
@check_env
def test_server_version():
    url = "http://localhost:6041/rest/sql"
    data = "select server_version()".encode("ascii")

    headers = {
        "Authorization": "Taosd " + default_token
    }
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

    headers = {
        "Authorization": "Taosd " + default_token
    }
    request = Request(url, data, headers)
    response = urlopen(request)
    resp = json.load(response)
    print("\n", resp)
    assert "code" in resp and resp["code"] != 0
