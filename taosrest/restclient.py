import json
import socket
from urllib.request import urlopen, Request

from iso8601 import parse_date

from .errors import ConnectError, ExecutionError, HTTPError

error_msgs = {
    400: "parameter error",
    401: "authorization error",
    404: "api not found",
    500: "internal error",
    503: "system resources is not sufficient. It is may be caused by a huge query."
}


class RestClient:
    """
     A wrapper for TDengine REST API.
     For detailed info about TDengine REST API refer https://docs.tdengine.com/reference/rest-api/
    """

    def __init__(self, url: str,
                 token: str = None,
                 database: str = None,
                 user: str = "root",
                 password: str = "taosdata",
                 timeout: int = None,
                 convert_timestamp=True):
        """
        Create a RestClient object.

        Parameters
        -----------
        - url : service address, required. for example: https://192.168.1.103:6041
        - token : cloud service token, optional
        - database: default database to use
        - user : username used to log in, optional
        - password : password used to log in, optional
        - timeout : the optional timeout parameter specifies a timeout in seconds for blocking operations
        - convert_timestamp: whether to convert timestamp from type str to type datetime with tzinfo.
            The default timezone is UTC. You can use method `datatime.astimezone()` to convert it to your local time.
        """
        # determine schema://host:post
        self._url = url.strip('/')
        if not self._url.startswith("http://") and not self._url.startswith("https://"):
            self._url = "http://" + self._url
        # timeout
        self._timeout = timeout if timeout is not None else socket._GLOBAL_DEFAULT_TIMEOUT

        # determine full URL to use and the header to user.
        if token:
            if not database:
                self._sql_utc_url = f"{self._url}/rest/sql?token={token}"
            else:
                self._sql_utc_url = f"{self._url}/rest/sql/{database}?token={token}"
            self._headers = {}
        else:
            self._login_url = f"{self._url}/rest/login/{user}/{password}"
            if not database:
                self._sql_utc_url = f"{self._url}/rest/sql"
            else:
                self._sql_utc_url = f"{self._url}/rest/sql/{database}"
            self._taosd_token = self.get_taosd_token()
            self._headers = {
                "Authorization": "Taosd " + self._taosd_token
            }
        self._convert_timestamp = convert_timestamp

    def get_taosd_token(self) -> str:
        """
        Get authorization token.
        """
        response = urlopen(self._login_url, timeout=self._timeout)
        self._check_status(response)

        resp = json.load(response)
        if resp["code"] != 0:
            raise ConnectError(resp["desc"], resp["code"])
        return resp["desc"]

    def sql(self, q: str) -> dict:
        """
        Execute sql and return the JSON response.

        If http status not equals to 200 or "code" in response not equals to 0, then Error will be raised.

        Parameters
        -----------
        q : SQL statement to execute. Can't be USE statement since REST api is stateless.
        """

        data = q.encode("utf8")
        request = Request(self._sql_utc_url, data, self._headers)
        response = urlopen(request, timeout=self._timeout)
        self._check_status(response)
        resp = json.load(response)
        if resp["code"] != 0:
            raise ExecutionError(resp["desc"], resp["code"])
        if self._convert_timestamp:
            self._convert_time(resp)
        return resp

    def _convert_time(self, resp: dict):
        """
        Convert timestamp in string format to python's datetime object with time zone info.
        """
        meta = resp["column_meta"]
        data = resp["data"]
        ts_cols = []
        for i in range(len(meta)):
            if meta[i][1] == "TIMESTAMP":
                ts_cols.append(i)

        if len(ts_cols) > 0:
            for row in data:
                for i in ts_cols:
                    if row[i]:
                        row[i] = parse_date(row[i])

    def _check_status(self, response):
        status = response.status
        if status != 200:
            msg = error_msgs.get(status)
            raise HTTPError(status, msg)
