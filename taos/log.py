# Copyright (c) 2024 TAOS Data, Inc.
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

# info

show_info = True
show_warn = True
show_debug = False
show_debug1 = False
show_debug2 = False
show_debug3 = False


# set show
def setting(info, warn, debug, debug1, debug2, debug3):
    global show_info, show_warn, show_debug, show_debug1, show_debug2, show_debug3
    show_info = info
    show_warn = warn
    show_debug = debug
    show_debug1 = debug1
    show_debug2 = debug2
    show_debug3 = debug3


# info
def info(msg):
    if show_info:
        print(msg)


# warn
def warn(msg):
    if show_warn:
        print(msg)


# debug
def debug(msg):
    if show_debug:
        print(msg)


# debug1
def debug1(msg):
    if show_debug1:
        print(msg)


# debug2
def debug2(msg):
    if show_debug2:
        print(msg)


# debug3
def debug3(msg):
    if show_debug3:
        print(msg)
