# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
    - timezone: str | datetime.tzinfo, optional, default None.
         When convert_timestamp is true, which timezone to used.
         When the type of timezone is str, it should be recognized by [pytz package](https://pypi.org/project/pytz/).
         When the timezone is None, system timezone will be used and the returned datetime object will be offset-naive (no tzinfo), otherwise the returned datetime will be offset-aware(with tzinfo)
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
