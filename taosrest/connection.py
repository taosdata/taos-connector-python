from typing import List

from .errors import NotSupportedError
from .cursor import TaosRestCursor
from .restclient import RestClient


class Result:
    def __init__(self, resp: dict):
        self.status: str = resp["status"]
        self.head: List = resp["head"]
        self.column_meta: List[list] = resp["column_meta"]
        self.data: List[list] = resp["data"]
        self.rows: int = resp["rows"]


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

    ############################################################################
    # Methods bellow are not PEP249 specified.
    # Add them for giving a similar programming experience as taos.Connection.
    ############################################################################

    @property
    def server_info(self):
        resp = self._c.sql("select server_version()")
        return resp["data"][0][0]

    def execute(self, sql):
        """
        execute sql usually INSERT statement and return affected row count.
        If there is not a column named "affected_rows" in response, then None is returned.
        """
        resp = self._c.sql(sql)
        if resp["head"] == ['affected_rows']:
            return resp["data"][0][0]
        else:
            return None

    def query(self, sql) -> Result:
        """
        execute sql and wrap the http response as Result object.
        """
        resp = self._c.sql(sql)
        return Result(resp)
