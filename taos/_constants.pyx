from taos._cinterface cimport TSDB_OPTION, TSDB_SML_PROTOCOL_TYPE, TSDB_SML_TIMESTAMP_TYPE, tmq_conf_res_t, tmq_res_t

cdef class TaosOption:
    """taos option"""
    Locale = TSDB_OPTION.TSDB_OPTION_LOCALE
    Charset = TSDB_OPTION.TSDB_OPTION_CHARSET
    Timezone = TSDB_OPTION.TSDB_OPTION_TIMEZONE
    ConfigDir = TSDB_OPTION.TSDB_OPTION_CONFIGDIR
    ShellActivityTimer = TSDB_OPTION.TSDB_OPTION_SHELL_ACTIVITY_TIMER
    UseAdapter = TSDB_OPTION.TSDB_OPTION_USE_ADAPTER
    MaxOptions = TSDB_OPTION.TSDB_MAX_OPTIONS

cdef class SmlPrecision:
    """Schemaless timestamp precision constants"""
    NOT_CONFIGURED = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_NOT_CONFIGURED
    HOURS = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_HOURS
    MINUTES = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_MINUTES
    SECONDS = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_SECONDS
    MILLI_SECONDS = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_MILLI_SECONDS
    MICRO_SECONDS = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_MICRO_SECONDS
    NANO_SECONDS = TSDB_SML_TIMESTAMP_TYPE.TSDB_SML_TIMESTAMP_NANO_SECONDS

cdef class SmlProtocol:
    """Schemaless protocol constants"""
    UNKNOWN_PROTOCOL = TSDB_SML_PROTOCOL_TYPE.TSDB_SML_UNKNOWN_PROTOCOL
    LINE_PROTOCOL = TSDB_SML_PROTOCOL_TYPE.TSDB_SML_LINE_PROTOCOL
    TELNET_PROTOCOL = TSDB_SML_PROTOCOL_TYPE.TSDB_SML_TELNET_PROTOCOL
    JSON_PROTOCOL = TSDB_SML_PROTOCOL_TYPE.TSDB_SML_JSON_PROTOCOL

cdef class TmqResultType:
    INVALID = tmq_res_t.TMQ_RES_INVALID
    DATA = tmq_res_t.TMQ_RES_DATA
    TABLE_META = tmq_res_t.TMQ_RES_TABLE_META
    METADATA = tmq_res_t.TMQ_RES_METADATA

class PrecisionEnum:
    """Precision enums"""
    Milliseconds = 0
    Microseconds = 1
    Nanoseconds = 2

class FieldType:
    """TDengine Field Types"""

    # type_code
    C_NULL = 0
    C_BOOL = 1
    C_TINYINT = 2
    C_SMALLINT = 3
    C_INT = 4
    C_BIGINT = 5
    C_FLOAT = 6
    C_DOUBLE = 7
    C_VARCHAR = 8
    C_BINARY = 8
    C_TIMESTAMP = 9
    C_NCHAR = 10
    C_TINYINT_UNSIGNED = 11
    C_SMALLINT_UNSIGNED = 12
    C_INT_UNSIGNED = 13
    C_BIGINT_UNSIGNED = 14
    C_JSON = 15
    # NULL value definition
    # NOTE: These values should change according to C definition in tsdb.h
    C_BOOL_NULL = 0x02
    C_TINYINT_NULL = -128
    C_TINYINT_UNSIGNED_NULL = 255
    C_SMALLINT_NULL = -32768
    C_SMALLINT_UNSIGNED_NULL = 65535
    C_INT_NULL = -2147483648
    C_INT_UNSIGNED_NULL = 4294967295
    C_BIGINT_NULL = -9223372036854775808
    C_BIGINT_UNSIGNED_NULL = 18446744073709551615
    C_FLOAT_NULL = float('nan')
    C_DOUBLE_NULL = float('nan')
    C_BINARY_NULL = bytearray([int("0xff", 16)])
    # Timestamp precision definition
    C_TIMESTAMP_MILLI = 0
    C_TIMESTAMP_MICRO = 1
    C_TIMESTAMP_NANO = 2
    C_TIMESTAMP_UNKNOWN = 3