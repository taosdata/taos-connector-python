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
   - host : str, optional.
       host to connect
   - user : str, optional.
       username used to log in
   - password : str, optional.
       password used to log in
    - port : int, optional.
       port to connect
   - timeout : int, optional.
       the optional timeout parameter specifies a timeout in seconds for blocking operations
   """
    return TaosRestConnection(**kwargs)
