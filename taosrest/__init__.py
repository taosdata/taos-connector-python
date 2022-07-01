from .connection import TaosRestConnection, Result
from .cursor import TaosRestCursor
from .restclient import RestClient
from .errors import *


def connect(**kwargs) -> TaosRestConnection:
    """
   Keyword Arguments
   ----------------------------
   - url: str, optional, default "http://localhost:6041"
        url to connect
   - token: str, optional, default None
        TDengine cloud Token, which is required only by TDengine cloud service
   - user : str, optional, default root
       username used to log in
   - password : str, optional, default taosdata
       password used to log in
   - database : str, optional, default None
        default database to use.
   - timeout : int, optional.
       the optional timeout parameter specifies a timeout in seconds for blocking operations
   - convert_timestamp: bool, optional, default true
        whether to convert timestamp in RFC3339 format to python datatime.

   Examples
   -----------------------------
    connect to cloud service
    ```python
    import taosrest, os
    url = os.environ("TDENGINE_CLOUD_URL")
    token = os.environ("TDENGINE_ClOUD_TOKEN")
    conn = taosrest.connect(url=url, token=token)
    ```
   connect to local taosAdapter
   ```python
   import taosrest
   conn = taosrest.connect(url="http://localhost:6041")
   ```
   """
    return TaosRestConnection(**kwargs)
