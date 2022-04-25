from .errors import NotSupportedError
from .cursor import TaosRestCursor
from .restclient import RestClient


class TaosRestConnection:
    """
    Implement [PEP 249 Connection API](https://peps.python.org/pep-0249/#connection-objects)
    """

    def __init__(self, **kwargs):
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
        self._host = kwargs["host"] if "host" in kwargs else "localhost"
        self._port = kwargs["port"] if "port" in kwargs else 6041
        self._user = kwargs["user"] if "user" in kwargs else "root"
        self._password = kwargs["password"] if "password" in kwargs else "taosdata"
        self._timeout = kwargs["timeout"] if "timeout" in kwargs else None
        self._c = RestClient(self._host, self._port, self._user, self._password, self._timeout)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        raise NotSupportedError()

    def cursor(self):
        return TaosRestCursor(self._c)

    @property
    def server_info(self):
        resp = self._c.sql("select server_version()")
        return resp["data"][0][0]
