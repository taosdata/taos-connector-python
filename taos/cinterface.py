# encoding:UTF-8

import ctypes
import platform
import inspect
from ctypes import *
import pytz

try:
    from typing import Any
except BaseException:
    pass

from .error import *
from .schemaless import *

_UNSUPPORTED = {}


# C interface class


class TaosOption:
    Locale = 0
    Charset = 1
    Timezone = 2
    ConfigDir = 3
    ShellActivityTimer = 4
    MaxOptions = 5


def _load_taos_linux():
    return ctypes.CDLL("libtaos.so")


def _load_taos_darwin():
    from ctypes.macholib.dyld import dyld_find as _dyld_find

    return ctypes.CDLL(_dyld_find("libtaos.dylib"))


def _load_taos_windows():
    return ctypes.windll.LoadLibrary("taos")


def _load_taos():
    load_func = {
        "Linux": _load_taos_linux,
        "Darwin": _load_taos_darwin,
        "Windows": _load_taos_windows,
    }
    pf = platform.system()
    if load_func[pf] is None:
        raise InterfaceError("unsupported platform: %s" % pf)
    try:
        return load_func[pf]()
    except Exception as err:
        raise InterfaceError("unable to load taos C library: %s" % err)


_libtaos = _load_taos()

_libtaos.taos_get_client_info.restype = c_char_p


def taos_get_client_info():
    # type: () -> str
    """Get client version info."""
    return _libtaos.taos_get_client_info().decode("utf-8")


IS_V3 = False

if taos_get_client_info().split(".")[0] < "3":
    from .field import CONVERT_FUNC, CONVERT_FUNC_BLOCK, TaosFields, TaosField, set_tz
else:
    from .field import CONVERT_FUNC, CONVERT_FUNC_BLOCK, TaosFields, TaosField, set_tz

    # use _v3s TaosField overwrite _v2s here, dont change import order
    from .field_v3 import CONVERT_FUNC_BLOCK_v3, TaosFields, TaosField
    from .constants import FieldType

    IS_V3 = True

_libtaos.taos_fetch_fields.restype = ctypes.POINTER(TaosField)

try:
    _libtaos.taos_init.restype = None
except Exception as err:
    _UNSUPPORTED["taos_init"] = err

_libtaos.taos_connect.restype = ctypes.c_void_p
_libtaos.taos_fetch_row.restype = ctypes.POINTER(ctypes.c_void_p)
_libtaos.taos_errstr.restype = ctypes.c_char_p

try:
    _libtaos.taos_subscribe.restype = ctypes.c_void_p
except Exception as err:
    _UNSUPPORTED["taos_subscribe"] = err

try:
    _libtaos.taos_consume.restype = ctypes.c_void_p
    _libtaos.taos_consume.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_consume"] = err

_libtaos.taos_fetch_lengths.restype = ctypes.POINTER(ctypes.c_int)
_libtaos.taos_free_result.restype = None
_libtaos.taos_query.restype = ctypes.POINTER(ctypes.c_void_p)

try:
    _libtaos.taos_stmt_errstr.restype = c_char_p
except AttributeError:
    None
finally:
    None

_libtaos.taos_options.restype = None
_libtaos.taos_options.argtypes = (c_int, c_void_p)


def taos_options(option, value):
    # type: (TaosOption, str) -> None
    _value = c_char_p(value.encode("utf-8"))
    _libtaos.taos_options(option, _value)


def taos_init():
    # type: () -> None
    """
    C: taos_init
    """
    _libtaos.taos_init()


_libtaos.taos_cleanup.restype = None


def taos_cleanup():
    # type: () -> None
    """Cleanup workspace."""
    _libtaos.taos_cleanup()


_libtaos.taos_get_server_info.restype = c_char_p
_libtaos.taos_get_server_info.argtypes = (c_void_p,)


def taos_get_server_info(connection):
    # type: (c_void_p) -> str
    """Get server version as string."""
    return _libtaos.taos_get_server_info(connection).decode("utf-8")


_libtaos.taos_close.restype = None
_libtaos.taos_close.argtypes = (c_void_p,)


def taos_close(connection):
    # type: (c_void_p) -> None
    """Close the TAOS* connection"""
    _libtaos.taos_close(connection)


_libtaos.taos_connect.restype = c_void_p
_libtaos.taos_connect.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect(host=None, user="root", password="taosdata", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """Create TDengine database connection.

    - host: server hostname/FQDN
    - user: user name
    - password: user password
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    # host
    try:
        _host = c_char_p(host.encode("utf-8")) if host is not None else None
    except AttributeError:
        raise AttributeError("host is expected as a str")

    # user
    try:
        _user = c_char_p(user.encode("utf-8"))
    except AttributeError:
        raise AttributeError("user is expected as a str")

    # password
    try:
        _password = c_char_p(password.encode("utf-8"))
    except AttributeError:
        raise AttributeError("password is expected as a str")

    # db
    try:
        _db = c_char_p(db.encode("utf-8")) if db is not None else None
    except AttributeError:
        raise AttributeError("db is expected as a str")

    # port
    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an uint16")

    connection = cast(_libtaos.taos_connect(_host, _user, _password, _db, _port), c_void_p)

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


_libtaos.taos_connect_auth.restype = c_void_p
_libtaos.taos_connect_auth.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16

_libtaos.taos_connect_auth.restype = c_void_p
_libtaos.taos_connect_auth.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect_auth(host=None, user="root", auth="", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """Connect server with auth token.

    - host: server hostname/FQDN
    - user: user name
    - auth: base64 encoded auth token
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    # host
    try:
        _host = c_char_p(host.encode("utf-8")) if host is not None else None
    except AttributeError:
        raise AttributeError("host is expected as a str")

    # user
    try:
        _user = c_char_p(user.encode("utf-8"))
    except AttributeError:
        raise AttributeError("user is expected as a str")

    # auth
    try:
        _auth = c_char_p(auth.encode("utf-8"))
    except AttributeError:
        raise AttributeError("password is expected as a str")

    # db
    try:
        _db = c_char_p(db.encode("utf-8")) if db is not None else None
    except AttributeError:
        raise AttributeError("db is expected as a str")

    # port
    try:
        _port = c_int(port)
    except TypeError:
        raise TypeError("port is expected as an int")

    connection = c_void_p(_libtaos.taos_connect_auth(_host, _user, _auth, _db, _port))

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


_libtaos.taos_query.restype = c_void_p
_libtaos.taos_query.argtypes = c_void_p, c_char_p


def taos_query(connection, sql):
    # type: (c_void_p, str) -> c_void_p
    """Run SQL

    - sql: str, sql string to run

    @return: TAOS_RES*, result pointer

    """
    try:
        ptr = c_char_p(sql.encode("utf-8"))
        res = c_void_p(_libtaos.taos_query(connection, ptr))
        errno = taos_errno(res)
        if errno != 0:
            errstr = taos_errstr(res)
            taos_free_result(res)
            raise ProgrammingError(errstr, errno)
        return res
    except AttributeError:
        raise AttributeError("sql is expected as a string")


async_query_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_query_a.restype = None
_libtaos.taos_query_a.argtypes = c_void_p, c_char_p, async_query_callback_type, c_void_p


def taos_query_a(connection, sql, callback, param):
    # type: (c_void_p, str, async_query_callback_type, c_void_p) -> c_void_p
    _libtaos.taos_query_a(connection, c_char_p(sql.encode("utf-8")), async_query_callback_type(callback), param)


async_fetch_rows_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_fetch_rows_a.restype = None
_libtaos.taos_fetch_rows_a.argtypes = c_void_p, async_fetch_rows_callback_type, c_void_p


def taos_fetch_rows_a(result, callback, param):
    # type: (c_void_p, async_fetch_rows_callback_type, c_void_p) -> c_void_p
    _libtaos.taos_fetch_rows_a(result, async_fetch_rows_callback_type(callback), param)


def taos_affected_rows(result):
    # type: (c_void_p) -> c_int
    """The affected rows after runing query"""
    return _libtaos.taos_affected_rows(result)


subscribe_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p, c_int)


# _libtaos.taos_subscribe.argtypes = c_void_p, c_int, c_char_p, c_char_p, subscribe_callback_type, c_void_p, c_int


def taos_subscribe(connection, restart, topic, sql, interval, callback=None, param=None):
    # type: (c_void_p, bool, str, str, c_int, subscribe_callback_type, c_void_p | None) -> c_void_p
    """Create a subscription
    @restart boolean,
    @sql string, sql statement for data query, must be a 'select' statement.
    @topic string, name of this subscription
    """
    if callback is not None:
        callback = subscribe_callback_type(callback)
    return c_void_p(
        _libtaos.taos_subscribe(
            connection,
            1 if restart else 0,
            c_char_p(topic.encode("utf-8")),
            c_char_p(sql.encode("utf-8")),
            callback,
            c_void_p(param),
            interval,
        )
    )


def taos_consume(sub):
    """Consume data of a subscription"""
    return c_void_p(_libtaos.taos_consume(sub))


try:
    _libtaos.taos_unsubscribe.restype = None
    _libtaos.taos_unsubscribe.argstype = c_void_p, c_int
except Exception as err:
    _UNSUPPORTED["taos_unsubscribe"] = err


def taos_unsubscribe(sub, keep_progress):
    """Cancel a subscription"""
    _libtaos.taos_unsubscribe(sub, 1 if keep_progress else 0)


def taos_use_result(result):
    """Use result after calling self.query, it's just for 1.6."""
    fields = []
    pfields = taos_fetch_fields_raw(result)
    for i in range(taos_field_count(result)):
        fields.append(
            {
                "name": pfields[i].name,
                "bytes": pfields[i].bytes,
                "type": pfields[i].type,
            }
        )

    return fields


_libtaos.taos_is_null.restype = c_bool
_libtaos.taos_is_null.argtypes = ctypes.c_void_p, c_int, c_int


def taos_is_null(result, row, col):
    return _libtaos.taos_is_null(result, row, col)


_libtaos.taos_fetch_block.restype = c_int
_libtaos.taos_fetch_block.argtypes = c_void_p, c_void_p


def taos_fetch_block_raw(result):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    return pblock, abs(num_of_rows)


if not IS_V3:
    pass
else:
    _libtaos.taos_get_column_data_offset.restype = ctypes.POINTER(ctypes.c_int)
    _libtaos.taos_get_column_data_offset.argtypes = ctypes.c_void_p, c_int


def taos_get_column_data_offset(result, field, rows):
    # Make sure to call taos_get_column_data_offset after taos_fetch_block()
    offsets = _libtaos.taos_get_column_data_offset(result, field)
    if not offsets:
        raise OperationalError("offsets empty, use taos_fetch_block before it")
    return offsets[:rows]


def taos_fetch_block_v3(result, fields=None, field_count=None):
    if fields is None:
        fields = taos_fetch_fields(result)
    if field_count is None:
        field_count = taos_field_count(result)
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    precision = taos_result_precision(result)
    blocks = [None] * field_count
    for i in range(len(fields)):
        data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
        if fields[i]["type"] not in CONVERT_FUNC_BLOCK_v3 and fields[i]["type"] not in CONVERT_FUNC_BLOCK:
            raise DatabaseError("Invalid data type returned from database")
        offsets = []
        is_null = []
        if fields[i]["type"] in (FieldType.C_VARCHAR, FieldType.C_NCHAR, FieldType.C_JSON):
            offsets = taos_get_column_data_offset(result, i, num_of_rows)
            blocks[i] = CONVERT_FUNC_BLOCK_v3[fields[i]["type"]](data, is_null, num_of_rows, offsets, precision)
        else:
            is_null = [taos_is_null(result, j, i) for j in range(num_of_rows)]
            blocks[i] = CONVERT_FUNC_BLOCK[fields[i]["type"]](data, is_null, num_of_rows, offsets, precision)

    return blocks, abs(num_of_rows)


def taos_fetch_block_v2(result, fields=None, field_count=None):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    precision = taos_result_precision(result)
    if fields is None:
        fields = taos_fetch_fields(result)
    if field_count is None:
        field_count = taos_field_count(result)
    blocks = [None] * field_count
    fieldLen = taos_fetch_lengths(result, field_count)
    for i in range(len(fields)):
        data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
        if fields[i]["type"] not in CONVERT_FUNC:
            raise DatabaseError("Invalid data type returned from database")
        is_null = [taos_is_null(result, j, i) for j in range(num_of_rows)]
        blocks[i] = CONVERT_FUNC_BLOCK[fields[i]["type"]](data, is_null, num_of_rows, fieldLen[i], precision)

    return blocks, abs(num_of_rows)


if not IS_V3:
    taos_fetch_block = taos_fetch_block_v2
else:
    taos_fetch_block = taos_fetch_block_v3

_libtaos.taos_fetch_row.restype = c_void_p
_libtaos.taos_fetch_row.argtypes = (c_void_p,)


def taos_fetch_row_raw(result):
    # type: (c_void_p) -> c_void_p
    row = c_void_p(_libtaos.taos_fetch_row(result))
    if row:
        return row
    return None


def taos_fetch_row(result, fields):
    # type: (c_void_p, Array[TaosField]) -> tuple(c_void_p, int)
    pblock = ctypes.c_void_p(0)
    pblock = taos_fetch_row_raw(result)
    if pblock:
        num_of_rows = 1
        precision = taos_result_precision(result)
        field_count = taos_field_count(result)
        blocks = [None] * field_count
        field_lens = taos_fetch_lengths(result, field_count)
        for i in range(field_count):
            data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
            if fields[i].type not in CONVERT_FUNC:
                raise DatabaseError("Invalid data type returned from database")
            if data is None:
                blocks[i] = [None]
            else:
                blocks[i] = CONVERT_FUNC[fields[i].type](data, [False], num_of_rows, field_lens[i], precision)
    else:
        return None, 0
    return blocks, abs(num_of_rows)


_libtaos.taos_free_result.argtypes = (c_void_p,)


def taos_free_result(result):
    # type: (c_void_p) -> None
    if result is not None:
        _libtaos.taos_free_result(result)


_libtaos.taos_field_count.restype = c_int
_libtaos.taos_field_count.argstype = (c_void_p,)


def taos_field_count(result):
    # type: (c_void_p) -> int
    return _libtaos.taos_field_count(result)


def taos_num_fields(result):
    # type: (c_void_p) -> int
    return _libtaos.taos_num_fields(result)


_libtaos.taos_fetch_fields.restype = c_void_p
_libtaos.taos_fetch_fields.argstype = (c_void_p,)


def taos_fetch_fields_raw(result):
    # type: (c_void_p) -> c_void_p
    return c_void_p(_libtaos.taos_fetch_fields(result))


def taos_fetch_fields(result):
    # type: (c_void_p) -> TaosFields
    fields = taos_fetch_fields_raw(result)
    count = taos_field_count(result)
    return TaosFields(fields, count)


def taos_fetch_lengths(result, field_count=None):
    # type: (c_void_p, int) -> Array[int]
    """Make sure to call taos_fetch_row or taos_fetch_block before fetch_lengths"""
    lens = _libtaos.taos_fetch_lengths(result)
    if field_count is None:
        field_count = taos_field_count(result)
    if not lens:
        raise OperationalError("field length empty, use taos_fetch_row/block before it")
    return lens[:field_count]


def taos_result_precision(result):
    # type: (c_void_p) -> c_int
    return _libtaos.taos_result_precision(result)


_libtaos.taos_errno.restype = c_int
_libtaos.taos_errno.argstype = (c_void_p,)


def taos_errno(result):
    # type: (ctypes.c_void_p) -> c_int
    """Return the error number."""
    return _libtaos.taos_errno(result)


_libtaos.taos_errstr.restype = c_char_p
_libtaos.taos_errstr.argstype = (c_void_p,)


def taos_errstr(result=c_void_p(None)):
    # type: (ctypes.c_void_p) -> str
    """Return the error styring"""
    return _libtaos.taos_errstr(result).decode("utf-8")


_libtaos.taos_stop_query.restype = None
_libtaos.taos_stop_query.argstype = (c_void_p,)


def taos_stop_query(result):
    # type: (ctypes.c_void_p) -> None
    """Stop current query"""
    return _libtaos.taos_stop_query(result)


try:
    _libtaos.taos_load_table_info.restype = c_int
    _libtaos.taos_load_table_info.argstype = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["taos_load_table_info"] = err


def taos_load_table_info(connection, tables):
    # type: (ctypes.c_void_p, str) -> None
    """Stop current query"""
    _check_if_supported()
    errno = _libtaos.taos_load_table_info(connection, c_char_p(tables.encode("utf-8")))
    if errno != 0:
        msg = taos_errstr()
        raise OperationalError(msg, errno)


_libtaos.taos_validate_sql.restype = c_int
_libtaos.taos_validate_sql.argstype = (c_void_p, c_char_p)


def taos_validate_sql(connection, sql):
    # type: (ctypes.c_void_p, str) -> None | str
    """Get taosd server info"""
    errno = _libtaos.taos_validate_sql(connection, ctypes.c_char_p(sql.encode("utf-8")))
    if errno != 0:
        msg = taos_errstr()
        return msg
    return None


_libtaos.taos_print_row.restype = c_int
_libtaos.taos_print_row.argstype = (c_char_p, c_void_p, c_void_p, c_int)


def taos_print_row(row, fields, num_fields, buffer_size=4096):
    # type: (ctypes.c_void_p, ctypes.c_void_p | TaosFields, int, int) -> str
    """Print an row to string"""
    p = ctypes.create_string_buffer(buffer_size)
    if isinstance(fields, TaosFields):
        _libtaos.taos_print_row(p, row, fields.as_ptr(), num_fields)
    else:
        _libtaos.taos_print_row(p, row, fields, num_fields)
    if p:
        return p.value.decode("utf-8")
    raise OperationalError("taos_print_row failed")


_libtaos.taos_select_db.restype = c_int
_libtaos.taos_select_db.argstype = (c_void_p, c_char_p)


def taos_select_db(connection, db):
    # type: (ctypes.c_void_p, str) -> None
    """Select database, eq to sql: use <db>"""
    res = _libtaos.taos_select_db(connection, ctypes.c_char_p(db.encode("utf-8")))
    if res != 0:
        raise DatabaseError("select database error", res)


_libtaos.taos_stmt_init.restype = c_void_p
_libtaos.taos_stmt_init.argstype = (c_void_p,)


def taos_stmt_init(connection):
    # type: (c_void_p) -> (c_void_p)
    """Create a statement query
    @param(connection): c_void_p TAOS*
    @rtype: c_void_p, *TAOS_STMT
    """
    return c_void_p(_libtaos.taos_stmt_init(connection))


_libtaos.taos_stmt_prepare.restype = c_int
_libtaos.taos_stmt_prepare.argstype = (c_void_p, c_char_p, c_int)


def taos_stmt_prepare(stmt, sql):
    # type: (ctypes.c_void_p, str) -> None
    """Prepare a statement query
    @stmt: c_void_p TAOS_STMT*
    """
    buffer = sql.encode("utf-8")
    res = _libtaos.taos_stmt_prepare(stmt, ctypes.c_char_p(buffer), len(buffer))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_close.restype = c_int
_libtaos.taos_stmt_close.argstype = (c_void_p,)


def taos_stmt_close(stmt):
    # type: (ctypes.c_void_p) -> None
    """Close a statement query
    @stmt: c_void_p TAOS_STMT*
    """
    res = _libtaos.taos_stmt_close(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_errstr.restype = c_char_p
    _libtaos.taos_stmt_errstr.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_errstr"] = err


def taos_stmt_errstr(stmt):
    # type: (ctypes.c_void_p) -> str
    """Get error message from stetement query
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    err = c_char_p(_libtaos.taos_stmt_errstr(stmt))
    if err:
        return err.value.decode("utf-8")


try:
    _libtaos.taos_stmt_set_tbname.restype = c_int
    _libtaos.taos_stmt_set_tbname.argstype = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_set_tbname"] = err


def taos_stmt_set_tbname(stmt, name):
    # type: (ctypes.c_void_p, str) -> None
    """Set table name of a statement query if exists.
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_set_tbname(stmt, c_char_p(name.encode("utf-8")))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_set_tbname_tags.restype = c_int
    _libtaos.taos_stmt_set_tbname_tags.argstype = (c_void_p, c_char_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_set_tbname_tags"] = err


def taos_stmt_set_tbname_tags(stmt, name, tags):
    # type: (c_void_p, str, c_void_p) -> None
    """Set table name with tags bind params.
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_set_tbname_tags(stmt, ctypes.c_char_p(name.encode("utf-8")), tags)

    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_is_insert.restype = c_int
    _libtaos.taos_stmt_is_insert.argstype = (c_void_p, POINTER(c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt_is_insert"] = err


def taos_stmt_is_insert(stmt):
    # type: (ctypes.c_void_p) -> bool
    """Set table name with tags bind params.
    @stmt: c_void_p TAOS_STMT*
    """
    is_insert = ctypes.c_int()
    res = _libtaos.taos_stmt_is_insert(stmt, ctypes.byref(is_insert))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)
    return is_insert == 0


try:
    _libtaos.taos_stmt_num_params.restype = c_int
    _libtaos.taos_stmt_num_params.argstype = (c_void_p, POINTER(c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt_num_params"] = err


def taos_stmt_num_params(stmt):
    # type: (ctypes.c_void_p) -> int
    """Params number of the current statement query.
    @stmt: TAOS_STMT*
    """
    num_params = ctypes.c_int()
    res = _libtaos.taos_stmt_num_params(stmt, ctypes.byref(num_params))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)
    return num_params.value


try:
    _libtaos.taos_stmt_bind_param.restype = c_int
    _libtaos.taos_stmt_bind_param.argstype = (c_void_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_param"] = err


def taos_stmt_bind_param(stmt, bind):
    # type: (ctypes.c_void_p, Array[TaosBind]) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_BIND*
    """
    # ptr = ctypes.cast(bind, POINTER(TaosBind))
    # ptr = pointer(bind)
    res = _libtaos.taos_stmt_bind_param(stmt, bind)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_bind_param_batch.restype = c_int
    _libtaos.taos_stmt_bind_param_batch.argstype = (c_void_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_param_batch"] = err


def taos_stmt_bind_param_batch(stmt, bind):
    # type: (ctypes.c_void_p, Array[TaosMultiBind]) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_BIND*
    """
    # ptr = ctypes.cast(bind, POINTER(TaosMultiBind))
    # ptr = pointer(bind)
    _check_if_supported()
    res = _libtaos.taos_stmt_bind_param_batch(stmt, bind)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_bind_single_param_batch.restype = c_int
    _libtaos.taos_stmt_bind_single_param_batch.argstype = (c_void_p, c_void_p, c_int)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_single_param_batch"] = err


def taos_stmt_bind_single_param_batch(stmt, bind, col):
    # type: (ctypes.c_void_p, Array[TaosMultiBind], c_int) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_MULTI_BIND*
    @col: column index
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_bind_single_param_batch(stmt, bind, col)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_add_batch.restype = c_int
    _libtaos.taos_stmt_add_batch.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_add_batch"] = err


def taos_stmt_add_batch(stmt):
    # type: (ctypes.c_void_p) -> None
    """Add current params into batch
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_add_batch(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_execute.restype = c_int
    _libtaos.taos_stmt_execute.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_execute"] = err


def taos_stmt_execute(stmt):
    # type: (ctypes.c_void_p) -> None
    """Execute a statement query
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_execute(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_use_result.restype = c_void_p
    _libtaos.taos_stmt_use_result.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_use_result"] = err


def taos_stmt_use_result(stmt):
    # type: (ctypes.c_void_p) -> None
    """Get result of the statement.
    @stmt: TAOS_STMT*
    """
    _check_if_supported()
    result = c_void_p(_libtaos.taos_stmt_use_result(stmt))
    if result is None:
        raise StatementError(taos_stmt_errstr(stmt))
    return result


try:
    _libtaos.taos_stmt_affected_rows.restype = c_int
    _libtaos.taos_stmt_affected_rows.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_affected_rows"] = err


def taos_stmt_affected_rows(stmt):
    # type: (ctypes.c_void_p) -> int
    """Get stmt affected rows.
    @stmt: TAOS_STMT*
    """
    _check_if_supported()
    return _libtaos.taos_stmt_affected_rows(stmt)


try:
    _libtaos.taos_schemaless_insert.restype = c_void_p
    _libtaos.taos_schemaless_insert.argstype = c_void_p, c_void_p, c_int, c_int, c_int
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert"] = err


def taos_schemaless_insert(connection, lines, protocol, precision):
    # type: (c_void_p, list[str] | tuple(str), SmlProtocol, SmlPrecision) -> int
    _check_if_supported()
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)
    res = c_void_p(_libtaos.taos_schemaless_insert(connection, p_lines, num_of_lines, protocol, precision))
    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)
    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


try:
    _libtaos.tmq_conf_new.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_conf_new"] = err


def tmq_conf_new():
    # type: () -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_conf_new())


try:
    _libtaos.tmq_conf_set.restype = c_int
    _libtaos.tmq_conf_set.argtypes = (c_void_p, c_char_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["tmq_conf_set"] = err


def tmq_conf_set(conf, key, value):
    # type: (c_void_p, c_char_p, c_char_p) -> None
    _check_if_supported()

    if not isinstance(value, str):
        raise TmqError(msg=f"fail to execute tmq_conf_set({key},{value}), {value} is not string type")

    res = _libtaos.tmq_conf_set(conf, ctypes.c_char_p(key.encode("utf-8")), ctypes.c_char_p(value.encode("utf-8")))
    if res != 0:
        raise TmqError(msg=f"fail to execute tmq_conf_set({key},{value}), code={res}", errno=res)


try:
    _libtaos.tmq_conf_destroy.argtypes = (c_void_p,)
    _libtaos.tmq_conf_destroy.restype = None
except Exception as err:
    _UNSUPPORTED["tmq_conf_destroy"] = err


def tmq_conf_destroy(conf):
    # type (c_void_p,) -> None
    _check_if_supported()
    _libtaos.tmq_conf_destroy(conf)


tmq_commit_cb = CFUNCTYPE(None, c_void_p, c_int, c_void_p)

try:
    _libtaos.tmq_conf_set_auto_commit_cb.argtypes = (c_void_p, tmq_commit_cb, c_void_p)
    _libtaos.tmq_conf_set_auto_commit_cb.restype = None
except Exception as err:
    _UNSUPPORTED["tmq_conf_set_auto_commit_cb"] = err


def tmq_conf_set_auto_commit_cb(conf, cb, param):
    # type (c_void_p, tmq_commit_cb, c_void_p) -> None
    _check_if_supported()
    _libtaos.tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb(cb), param)


try:
    _libtaos.tmq_consumer_new.restype = c_void_p
    _libtaos.tmq_consumer_new.argtypes = (c_void_p, c_char_p, c_int)
except Exception as err:
    _UNSUPPORTED["tmq_consumer_new"] = err


def tmq_consumer_new(conf, errstrlen=0):
    # type (c_void_p, c_char_p, c_int) -> c_void_p
    _check_if_supported()
    buf = ctypes.create_string_buffer(errstrlen)
    tmq = cast(_libtaos.tmq_consumer_new(conf, buf, errstrlen), c_void_p)
    if tmq.value is None:
        raise TmqError("failed on tmq_consumer_new")
    return tmq


try:
    _libtaos.tmq_list_new.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_list_new"] = err


def tmq_list_new():
    # type () -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_list_new())


try:
    _libtaos.tmq_list_append.restype = c_int
    _libtaos.tmq_list_append.argtypes = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["tmq_list_append"] = err


def tmq_list_append(list, topic):
    # type (c_void_p, c_char_p) -> None
    _check_if_supported()

    if not isinstance(topic, str):
        raise TmqError(f"topic value: {topic} is not string type")

    res = _libtaos.tmq_list_append(list, ctypes.c_char_p(topic.encode("utf-8")))
    return res


try:
    _libtaos.tmq_list_destroy.restype = None
    _libtaos.tmq_list_destroy.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_destroy"] = err


def tmq_list_destroy(list):
    _check_if_supported()
    # type (c_void_p,) -> None
    _libtaos.tmq_list_destroy(list)


try:
    _libtaos.tmq_list_get_size.restype = c_int
    _libtaos.tmq_list_get_size.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_get_size"] = err

try:
    _libtaos.tmq_list_to_c_array.restype = ctypes.POINTER(ctypes.c_char_p)
    _libtaos.tmq_list_to_c_array.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_to_c_array"] = err


def tmq_list_to_c_array(list):
    # type (c_void_p,) -> [string]
    _check_if_supported()
    _check_if_supported("tmq_list_get_size")
    c_array = _libtaos.tmq_list_to_c_array(list)
    size = _libtaos.tmq_list_get_size(list)
    res = []
    for i in range(size):
        res.append(c_array[i].decode("utf-8"))
    return res


try:
    _libtaos.tmq_subscribe.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_subscribe.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_subscribe"] = err


def tmq_subscribe(tmq, list):
    # type (c_void_p, c_void_p) -> None
    _check_if_supported()
    res = _libtaos.tmq_subscribe(tmq, list)
    if res != 0:
        raise TmqError(msg="failed on tmq_subscribe()", errno=res)


try:
    _libtaos.tmq_unsubscribe.argtypes = (c_void_p,)
    _libtaos.tmq_unsubscribe.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_unsubscribe"] = err


def tmq_unsubscribe(tmq):
    # type (c_void_p,) -> None
    _check_if_supported()
    res = _libtaos.tmq_unsubscribe(tmq)
    if res == -1:
        # -1 means empty subscription topic list..
        # tmq_unsubscribe will subscribe a empty topic list to clear the consumer task.
        return
    if res != 0:
        raise TmqError(msg=f"failed on tmq_unsubscribe() {res}", errno=res)


try:
    _libtaos.tmq_subscription.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_subscription.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_subscription"] = err


def tmq_subscription(tmq):
    # type (c_void_p, c_void_p) -> [string]
    _check_if_supported()
    topics = tmq_list_new()
    res = _libtaos.tmq_subscription(tmq, byref(topics))
    if res != 0:
        raise TmqError(msg="failed on tmq_subscription()", errno=res)
    res = tmq_list_to_c_array(topics)
    tmq_list_destroy(topics)
    return res


try:
    _libtaos.tmq_consumer_poll.argtypes = (c_void_p, c_int64)
    _libtaos.tmq_consumer_poll.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_consumer_poll"] = err


def tmq_consumer_poll(tmq, wait_time):
    # type (c_void_p, c_int64) -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_consumer_poll(tmq, wait_time))


try:
    _libtaos.tmq_consumer_close.argtypes = (c_void_p,)
    _libtaos.tmq_consumer_close.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_consumer_close"] = err


def tmq_consumer_close(tmq):
    # type (c_void_p,) -> None
    _check_if_supported()
    res = _libtaos.tmq_consumer_close(tmq)
    if res != 0:
        raise TmqError(msg="failed on tmq_consumer_close()", errno=res)


try:
    _libtaos.tmq_commit_sync.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_commit_sync.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_commit"] = err


def tmq_commit_sync(tmq, offset):
    # type: (c_void_p, c_void_p) -> None
    _check_if_supported()
    res = _libtaos.tmq_commit_sync(tmq, offset)
    if res != 0:
        raise TmqError(msg="failed on tmq_commit_sync()", errno=res)


try:
    _libtaos.tmq_get_topic_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_topic_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_topic_name"] = err


def tmq_get_topic_name(res):
    # type: (c_void_p,) -> str
    _check_if_supported()
    return _libtaos.tmq_get_topic_name(res).decode("utf-8")


try:
    _libtaos.tmq_get_vgroup_id.argtypes = (c_void_p,)
    _libtaos.tmq_get_vgroup_id.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_get_vgroup_id"] = err


def tmq_get_vgroup_id(res):
    # type: (c_void_p,) -> int
    _check_if_supported()
    return _libtaos.tmq_get_vgroup_id(res)


try:
    _libtaos.tmq_get_table_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_table_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_table_name"] = err


def tmq_get_table_name(res):
    # type: (c_void_p,) -> str
    _check_if_supported()
    tb_name = _libtaos.tmq_get_table_name(res)
    if tb_name:
        return tb_name.decode("utf-8")
    else:
        print("change msg.with.table.name to true in tmq config")


try:
    _libtaos.tmq_get_db_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_db_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_db_name"] = err


def tmq_get_db_name(res):
    # type: (c_void_p,) -> str
    _check_if_supported()
    return _libtaos.tmq_get_db_name(res).decode("utf-8")


def _check_if_supported(func=None):
    if not func:
        func = inspect.stack()[1][3]
    if func in _UNSUPPORTED:
        raise InterfaceError(
            "C function %s is not supported in v%s: %s" % (func, taos_get_client_info(), _UNSUPPORTED[func])
        )


def unsupported_methods():
    for m, e in range(_UNSUPPORTED):
        print("unsupported %s: %s", m, e)


class CTaosInterface(object):
    def __init__(self, config=None, tz=None):
        """
        Function to initialize the class
        @host     : str, hostname to connect
        @user     : str, username to connect to server
        @password : str, password to connect to server
        @db       : str, default db to use when log in
        @config   : str, config directory

        @rtype    : None
        """
        if config is not None:
            taos_options(TaosOption.ConfigDir, config)

        if tz is not None:
            set_tz(pytz.timezone(tz))
            taos_options(TaosOption.Timezone, tz)

    @property
    def config(self):
        """Get current config"""
        return self._config

    def connect(self, host=None, user="root", password="taosdata", db=None, port=0):
        """
        Function to connect to server

        @rtype: c_void_p, TDengine handle
        """

        return taos_connect(host, user, password, db, port)


if __name__ == "__main__":
    cinter = CTaosInterface()
    conn = cinter.connect()
    result = cinter.query(conn, "show databases")

    print("Query Affected rows: {}".format(cinter.affected_rows(result)))

    fields = taos_fetch_fields_raw(result)

    data, num_of_rows = taos_fetch_block(result, fields)

    print(data)

    cinter.free_result(result)
    cinter.close(conn)
