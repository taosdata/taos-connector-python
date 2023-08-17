from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libcpp cimport bool

cdef extern from "taos.h":
    ctypedef void TAOS
    ctypedef void TAOS_RES
    ctypedef void **TAOS_ROW
    ctypedef struct TAOS_FIELD:
        char name[65]
        int8_t type
        int32_t bytes
    int TSDB_DATA_TYPE_NULL
    int TSDB_DATA_TYPE_BOOL
    int TSDB_DATA_TYPE_TINYINT
    int TSDB_DATA_TYPE_SMALLINT
    int TSDB_DATA_TYPE_INT
    int TSDB_DATA_TYPE_BIGINT
    int TSDB_DATA_TYPE_FLOAT
    int TSDB_DATA_TYPE_DOUBLE
    int TSDB_DATA_TYPE_VARCHAR
    int TSDB_DATA_TYPE_TIMESTAMP
    int TSDB_DATA_TYPE_NCHAR
    int TSDB_DATA_TYPE_UTINYINT
    int TSDB_DATA_TYPE_USMALLINT
    int TSDB_DATA_TYPE_UINT
    int TSDB_DATA_TYPE_UBIGINT
    int TSDB_DATA_TYPE_JSON
    int TSDB_DATA_TYPE_VARBINARY
    int TSDB_DATA_TYPE_DECIMAL
    int TSDB_DATA_TYPE_BLOB
    int TSDB_DATA_TYPE_MEDIUMBLOB
    int TSDB_DATA_TYPE_BINARY
    int TSDB_DATA_TYPE_GEOMETRY
    int TSDB_DATA_TYPE_MAX
    int taos_init()
    bool taos_is_null(TAOS_RES *res, int32_t row, int32_t col)
    TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)
    int taos_field_count(TAOS_RES *res)
    int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
    int taos_result_precision(TAOS_RES *res)
    int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex)
    int taos_errno(TAOS_RES *res)
    char *taos_errstr(TAOS_RES *res)
    TAOS *taos_connect(const char *ip, const char *user, const char *password, const char *db, uint16_t port)
    void taos_close(TAOS *taos)