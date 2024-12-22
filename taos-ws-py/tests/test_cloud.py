import os
import sys
import taosws

def test_ws_connect(token):
    print("-" * 40)
    print("test_ws_connect")
    # get evn
    #token = os.environ.get("CLOUD_TOKEN")
    print(token)
    conn = taosws.connect("%s?token=%s" % ('wss://gw.cloud.taosdata.com', token))
    res = conn.query_with_req_id("show databases", 1)
    dbs = [row[0] for row in res]
    print(dbs)
    # check sys db exist
    assert "information_schema" in dbs
    assert "performance_schema" in dbs
    print("test_ws_connect done")
    print("-" * 40)

if __name__ == "__main__":
    if len(sys.argv) == 0:
        print("need token argument passed.\n")
    else:
        test_ws_connect(sys.argv[0])