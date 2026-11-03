# Copyright (c) 2023 TAOS Data, Inc.
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

from taos import *


def test_gen_req_id():
    req_id = utils.gen_req_id()
    print(req_id)
    print(hex(req_id))


def test_murmurhash3_32_tail_length_1():
    case1 = "a".encode("utf-8")
    hashed = utils.murmurhash3_32(case1, len(case1))
    print(hashed)


def test_murmurhash3_32_tail_length_2():
    case2 = "python".encode("utf-8")
    hashed = utils.murmurhash3_32(case2, len(case2))
    print(hashed)


def test_murmurhash3_32_tail_length_3():
    case3 = "123".encode("utf-8")
    hashed = utils.murmurhash3_32(case3, len(case3))
    print(hashed)
