from .connection import TaosRestConnection


def connect(**kwargs) -> TaosRestConnection:
    """
   Keyword Arguments
   ----------------------------
   - host : str, optional.
       host to connect
   - port : int, optional.
       port to connect
   - user : str, optional.
       username used to log in
   - password : str, optional.
       password used to log in
   - timeout : int, optional.
       the optional timeout parameter specifies a timeout in seconds for blocking operations
   """
    return TaosRestConnection(**kwargs)
