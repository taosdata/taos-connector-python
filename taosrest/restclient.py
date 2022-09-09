import datetime
import json
import socket
from urllib.request import urlopen, Request

from iso8601 import parse_date
from pytz import timezone as Timezone
from typing import Union

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
                 convert_timestamp: bool = True,
                 timezone: Union[str, datetime.tzinfo] = None):
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
        - timezone: str | datetime.tzinfo, optional, default None.
            When convert_timestamp is true, which timezone to used.
            When the type of timezone is str, it should be recognized by [pytz package](https://pypi.org/project/pytz/).
            When the timezone is None, system timezone will be used and the returned datetime object will be offset-naive (no tzinfo), otherwise the returned datetime will be offset-aware(with tzinfo)
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
                self._sql_utc_url = f"{self._url}/rest/sqlutc?token={token}"
            else:
                self._sql_utc_url = f"{self._url}/rest/sqlutc/{database}?token={token}"
            self._headers = {}
        else:
            self._login_url = f"{self._url}/rest/login/{user}/{password}"
            if not database:
                self._sql_utc_url = f"{self._url}/rest/sqlutc"
            else:
                self._sql_utc_url = f"{self._url}/rest/sqlutc/{database}"
            self._taosd_token = self.get_taosd_token()
            self._headers = {
                "Authorization": "Taosd " + self._taosd_token
            }

        self._convert_timestamp = convert_timestamp

        try:
            data = "select 1".encode("utf8")
            request = Request(self._sql_utc_url, data, self._headers)
            response = urlopen(request, timeout=self._timeout)
        except:
            self._sql_utc_url = self._sql_utc_url.replace("sqlutc", "sql")

        if timezone is None:
            self._timezone = None
        elif isinstance(timezone, str):
            self._timezone = Timezone(timezone)
        elif isinstance(timezone, datetime.tzinfo):
            self._timezone = timezone
        else:
            raise TypeError("timezone argument must be an instance of str or tzinfo")

    def get_taosd_token(self) -> str:
        """
        Get authorization token.
        """
        response = urlopen(self._login_url, timeout=self._timeout)
        self._check_status(response)

        resp = json.load(response)
        if "status" in resp:
            if resp["status"] != "succ":
                raise ConnectError(resp["desc"], status=resp["status"])
        else:
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
        if "status" in resp:
            # v2
            if resp["status"] != "succ":
                raise ConnectError(resp["desc"], status=resp["status"])
        else:
            if resp["code"] != 0:
                raise ConnectError(resp["desc"], resp["code"])
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
            if meta[i][1] == "TIMESTAMP" or meta[i][1] == 9:
                ts_cols.append(i)
        if len(ts_cols) == 0:
            return
        if self._timezone:
            for row in data:
                for i in ts_cols:
                    if row[i]:
                        dt = parse_date(row[i])  # UTC
                        row[i] = dt.astimezone(self._timezone)
        else:
            for row in data:
                for i in ts_cols:
                    if row[i]:
                        dt = parse_date(row[i])  # UTC
                        dt = dt.astimezone()  # local
                        row[i] = dt.replace(tzinfo=None)  # naive datetime

    def _check_status(self, response):
        status = response.status
        if status != 200:
            msg = error_msgs.get(status)
            raise HTTPError(status, msg)
