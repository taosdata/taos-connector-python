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
        - url : str, optional.
            url to connect
        - token : str, optional
            cloud service token
        - user : str, optional.
            username used to log in
        - password : str, optional.
            password used to log in
        - timeout : int, optional.
            the optional timeout parameter specifies a timeout in seconds for blocking operations
        """
        self._url = kwargs["url"] if "url" in kwargs else "http://localhost:6041"
        self._user = kwargs["user"] if "user" in kwargs else "root"
        self._password = kwargs["password"] if "password" in kwargs else "taosdata"
        self._timeout = kwargs["timeout"] if "timeout" in kwargs else None
        self._token = kwargs["token"] if "token" in kwargs else None
        self._c = RestClient(self._url, token=self._token, user=self._user, password=self._password, timeout=self._timeout)

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
