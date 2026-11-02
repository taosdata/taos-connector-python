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

from iso8601 import parse_date


def test_parse_time():
    s1 = "2018-10-03T14:38:15+08:00"
    t1 = parse_date(s1)
    print("\n", repr(t1))
    assert t1.isoformat() == "2018-10-03T14:38:15+08:00"
    s2 = "2018-10-03T14:38:16.8+08:00"
    t2 = parse_date(s2)
    assert t2.isoformat() == "2018-10-03T14:38:16.800000+08:00"
    s3 = "2018-10-03T14:38:16.65+08:00"
    t3 = parse_date(s3)
    assert t3.isoformat() == "2018-10-03T14:38:16.650000+08:00"
