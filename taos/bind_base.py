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

# encoding:UTF-8

from ctypes import *
from datetime import datetime, timezone

from taos.constants import FieldType
from taos.precision import PrecisionEnum, PrecisionError


from taos.field import DEFAULT_TIMEZONE, get_tz


def parse_datetime_to_utc_timestamp(dt, tz=DEFAULT_TIMEZONE) -> int:
    if isinstance(dt, str):
        dt_naive = datetime.fromisoformat(dt)
    elif isinstance(dt, datetime):
        dt_naive = dt
    else:
        raise TypeError(f"dt type error, expected str or datetime type but got {type(dt)}.")
    #
    if dt_naive.tzinfo is None or dt_naive.tzinfo.utcoffset(dt_naive) is None:
        selected_tz = get_tz() if tz is DEFAULT_TIMEZONE else tz
        if selected_tz is not None:
            if hasattr(selected_tz, "localize"):
                dt_aware = selected_tz.localize(dt_naive)
            else:
                dt_aware = dt_naive.replace(tzinfo=selected_tz)
        else:
            dt_aware = dt_naive
    else:
        dt_aware = dt_naive.astimezone(timezone.utc)
    #
    result = int(dt_aware.timestamp())
    return result


def datetime_to_timestamp(value, precision, is_null_type=0, tz=DEFAULT_TIMEZONE):
    # type: (datetime | float | int | str | c_int64, PrecisionEnum, int) -> c_int64
    if is_null_type > 0 or value is None:
        return FieldType.C_BIGINT_NULL

    if type(value) is datetime or isinstance(value, str):
        utc_timestamp = parse_datetime_to_utc_timestamp(value, tz)
        if precision == PrecisionEnum.Milliseconds:
            ret_value = c_int64(utc_timestamp * 10**3)
            return ret_value
        elif precision == PrecisionEnum.Microseconds:
            ret_value = c_int64(utc_timestamp * 10**6)
            return ret_value
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif type(value) is float:
        if precision == PrecisionEnum.Milliseconds:
            return c_int64(round(value * 10**3))
        elif precision == PrecisionEnum.Microseconds:
            return c_int64(round(value * 10**6))
        else:
            raise PrecisionError("time float do not support nanosecond precision")
    elif isinstance(value, int) and not isinstance(value, bool):
        return c_int64(value)
    elif isinstance(value, c_int64):
        return value
    return FieldType.C_BIGINT_NULL
