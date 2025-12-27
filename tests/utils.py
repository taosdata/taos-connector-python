import os

IS_WS = os.getenv("TDENGINE_DRIVER", "native").lower() == "websocket"

PORT = 6030 if not IS_WS else 6041


def tear_down_database(conn, db_name):
    conn.execute("DROP DATABASE IF EXISTS %s" % db_name)
