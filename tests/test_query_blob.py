# Copyright (c) 2025 TAOS Data, Inc.
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

from datetime import datetime

from utils import tear_down_database
from taos import utils, IS_V3
from taos.error import InterfaceError
import taos


def test_query():
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    conn = taos.connect()
    db_name = "test_query_py"
    try:
        conn.execute("drop database if exists %s" % db_name)
        conn.execute("create database if not exists %s" % db_name)
        conn.execute("use %s" % db_name)
        conn.execute("create table if not exists tb1 (ts timestamp, v blob) tags(jt json)")
        n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null)')
        print("inserted %d rows" % n)
        result = conn.query("select * from tb1 order by ts")
        try:
            fields = result.fields
            print("fields: ", fields)
            assert fields.count == 3

            results = result.fetch_all()
            print("results: ", results)
        finally:
            # release the result set even when an assertion above raises
            result.close()
        n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now + 10s, "xxxxxxxxxxxxxxxxxxx")')
        print("inserted %d rows" % n)
        result = conn.query("select * from tb1 order by ts")
        try:
            fields = result.fields
            print("fields: ", fields)
            assert fields.count == 3
            results = result.fetch_all()
            print("results: ", results)
        finally:
            # release the result set even when an assertion above raises
            result.close()

        n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now + 20s, "\\x7f8290")')
        print("inserted %d rows" % n)
        result = conn.query("select * from tb1 order by ts")
        try:
            fields = result.fields
            print("fields: ", fields)
            assert fields.count == 3
            results = result.fetch_all()
            print("results: ", results)
            assert results[0][1] is None
            assert results[1][1] == b"xxxxxxxxxxxxxxxxxxx"
            assert results[2][1] == b"\x7f\x82\x90"
            assert len(results) == 3
        finally:
            result.close()
    finally:
        # tear_down_database executes DROP DATABASE and may raise; nest the close
        # so the connection is released on that path too
        try:
            tear_down_database(conn, db_name)
        finally:
            conn.close()


def test_query_large_blob():
    """blob values larger than 65535 bytes must be read completely (4-byte length header)"""
    conn = taos.connect()
    db_name = "test_large_blob_py"
    try:
        conn.execute("drop database if exists %s" % db_name)
        conn.execute("create database if not exists %s" % db_name)
        conn.execute("use %s" % db_name)
        conn.execute("create table tb1 (ts timestamp, v blob)")

        # two rows with payloads > 65535 bytes to cover both length header and row stride
        payload1 = bytes((i * 7 + 13) % 256 for i in range(100 * 1024))
        payload2 = bytes((i * 3 + 1) % 256 for i in range(70 * 1024))
        n = conn.execute('insert into tb1 values(now, "\\x%s")' % payload1.hex())
        print("inserted %d rows" % n)
        assert n == 1, "insert large blob row 1 failed: affected_rows=%s" % n
        n = conn.execute('insert into tb1 values(now + 10s, "\\x%s")' % payload2.hex())
        print("inserted %d rows" % n)
        assert n == 1, "insert large blob row 2 failed: affected_rows=%s" % n

        # explicit short payload with embedded NUL: guards against c_char_p-style
        # NUL-truncation regressions in the blob decode path (field.py uses
        # buffer.raw[:rbyte] precisely so that NUL bytes survive)
        n = conn.execute('insert into tb1 values(now + 20s, "\\x010002")')
        print("inserted %d rows" % n)
        assert n == 1, "insert embedded-NUL blob failed: affected_rows=%s" % n

        result = conn.query("select v from tb1 order by ts")
        try:
            results = result.fetch_all()
            assert len(results) == 3

            # type-safe failure messages: if the 4-byte length header regresses the driver
            # may return None (header misread as 0/-1 -> null branch), and len(None) would
            # raise TypeError, masking the real mismatch — describe type and length instead
            def describe(v):
                return "type=%s len=%s" % (type(v).__name__, len(v) if isinstance(v, (bytes, bytearray)) else "N/A")

            assert results[0][0] == payload1, "large blob row 1 mismatch: %s" % describe(results[0][0])
            assert results[1][0] == payload2, "large blob row 2 mismatch: %s" % describe(results[1][0])
            assert results[2][0] == b"\x01\x00\x02", "blob with embedded NUL mismatch: %s" % describe(results[2][0])
        finally:
            # release the result set even when an assertion above raises
            result.close()

        # also cover the cursor.fetchall path
        cur = conn.cursor()
        try:
            cur.execute("select v from tb1 order by ts")
            rows = cur.fetchall()
            assert len(rows) == 3
            assert rows[0][0] == payload1
            assert rows[1][0] == payload2
            assert rows[2][0] == b"\x01\x00\x02"
        finally:
            cur.close()
    finally:
        # same nesting as above: close even if the DROP DATABASE inside
        # tear_down_database raises
        try:
            tear_down_database(conn, db_name)
        finally:
            conn.close()


