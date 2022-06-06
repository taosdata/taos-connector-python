from urllib.request import urlopen, Request
import taos
import json

default_token = "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"


def test_login():
    if taos.IS_V3:
        return
    response = urlopen("http://localhost:6041/rest/login/root/taosdata")
    resp = json.load(response)
    print()
    print(resp)
    assert "status" in resp and resp["status"] == "succ"
    assert "code" in resp and resp["code"] == 0
    assert "desc" in resp and resp["desc"] == default_token


def test_wrong_password():
    """
    {'status': 'error', 'code': 3, 'desc': 'Authentication failure'}
    """
    if taos.IS_V3:
        return
    response = urlopen("http://localhost:6041/rest/login/root/taosdatax")
    resp = json.load(response)
    print("\n=======================================")
    print(resp)


def test_server_version():
    """
    {'status': 'succ', 'head': ['server_version()'], 'column_meta': [['server_version()', 8, 8]], 'data': [['2.4.0.16']], 'rows': 1}
    """
    if taos.IS_V3:
        return
    url = "http://localhost:6041/rest/sqlutc"
    data = "select server_version()".encode("ascii")

    headers = {
        "Authorization": "Taosd " + default_token
    }
    request = Request(url, data, headers)
    response = urlopen(request)
    resp = json.load(response)
    assert "status" in resp and resp["status"] == "succ"
    assert "head" in resp and resp["head"][0] == "server_version()"
    assert resp["rows"] == 1
    assert len(resp["column_meta"]) == 1
    assert len(resp["data"]) == 1


def test_wrong_sql():
    """
    {'status': 'error', 'code': 866, 'desc': 'Table does not exist'}
    """
    if taos.IS_V3:
        return
    url = "http://localhost:6041/rest/sqlutc"
    data = "select * from nodb.notable".encode("utf8")

    headers = {
        "Authorization": "Taosd " + default_token
    }
    request = Request(url, data, headers)
    response = urlopen(request)
    resp = json.load(response)
    print("\n", resp)
    assert "status" in resp and resp["status"] == "error"




