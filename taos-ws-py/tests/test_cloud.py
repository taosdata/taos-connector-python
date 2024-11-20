import taosws

def test_ws_connect():
    print("-" * 40)
    print("test_ws_connect")
    conn = taosws.connect("%s?token=%s" % ('wss://gw.cloud.taosdata.com', '68d41c020d2e9e3dcac18140460d1bcfd82d642d'))
    r = conn.query_with_req_id("show dnodes", 1)
    print("r: ", r.fields)
    print("test_ws_connect done")
    print("-" * 40)

if __name__ == "__main__":
    test_ws_connect()
