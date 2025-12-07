import os

IS_WS = os.getenv("TDENGINE_DRIVER", "native").lower() == "websocket"

PORT = 6030 if not IS_WS else 6041
