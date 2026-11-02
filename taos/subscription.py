# Copyright (c) 2020 TAOS Data, Inc.
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

from taos.result import TaosResult
from taos.cinterface import taos_consume, taos_unsubscribe
from taos.error import OperationalError
from taos.field import DEFAULT_TIMEZONE


class TaosSubscription(object):
    """TDengine subscription object"""

    def __init__(self, sub, with_callback=False, timezone=DEFAULT_TIMEZONE):
        self._sub = sub
        self._with_callback = with_callback
        self._timezone = timezone

    def consume(self):
        """Consume rows of a subscription"""
        if self._sub is None:
            raise OperationalError("Invalid use of consume")
        if self._with_callback:
            raise OperationalError("DONOT use consume method in an subscription with callback")
        result = taos_consume(self._sub)
        return TaosResult(result, timezone=self._timezone)

    def close(self, keepProgress=True):
        """Close the Subscription."""
        if self._sub is None:
            return False

        taos_unsubscribe(self._sub, keepProgress)
        self._sub = None
        return True

    def __del__(self):
        self.close()
