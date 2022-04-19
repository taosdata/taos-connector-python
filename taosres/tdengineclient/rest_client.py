from urllib.request import urlopen, Request
from .errors import ConnectionError
import json


class RestClient:
    def __init__(self, host, port, user, password):
        self._login_url = f"http://{host}:{port}/rest/login/{user}/{password}"
        self._sqlutc_url = f"http://{host}:{port}/rest/sqlutc"
        self._token = self._get_token()

    def _get_token(self):
        response = urlopen(self._login_url)
        resp = json.load(response)
        if resp["code"] != 0:
            raise ConnectionError(resp["desc"], resp["code"])
        return resp["desc"]
