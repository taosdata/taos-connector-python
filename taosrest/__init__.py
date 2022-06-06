from .connection import TaosRestConnection
from .cursor import TaosRestCursor
from .restclient import RestClient
from .errors import *

threadsafety = 0
paramstyle = "pyformat"


def connect(**kwargs) -> TaosRestConnection:
    """
   Keyword Arguments
   ----------------------------
   - url: str,
        url to connect
   - token: str,
        TDengine cloud Token, which is required only by TDengine cloud service
   - user : str, optional.
       username used to log in
   - password : str, optional.
       password used to log in
   - timeout : int, optional.
       the optional timeout parameter specifies a timeout in seconds for blocking operations

   Examples
   -----------------------------
    connect to cloud service
    ```python
    import taosrest, os
    url = os.environ("TDENGINE_CLOUD_URL")
    token = os.environ("TDENGINE_ClOUD_TOKEN")
    conn = taosrest.connect(url=url, token=token)
    ```
   connect to local TDengine
   ```python
   import taosrest
   conn = taosrest.connect(url="http://ocalhost:6041")
   ```

   """
    return TaosRestConnection(**kwargs)
