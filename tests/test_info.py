# Copyright (c) 2021 TAOS Data, Inc.
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

import taos
from taos.cinterface import *

from taos import *

import pytest


@pytest.fixture
def conn():
    return connect()


def test_client_info():
    print("client info: %s" % taos_get_client_info())
    pass


def test_server_info(conn):
    # type: (TaosConnection) -> None
    print("conn client info: %s" % conn.client_info)
    print("conn server info: %s" % conn.server_info)
    pass


def test_log():
    taos.log.setting(True, True, True, True, True, True)
    taos.log.info("log is info")
    taos.log.debug("log is debug")
    taos.log.debug1("log is debug")
    taos.log.debug2("log is debug")
    taos.log.debug3("log is debug")


if __name__ == "__main__":
    test_client_info()
    test_server_info(connect())
