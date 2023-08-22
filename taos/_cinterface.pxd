from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, uintptr_t
from libcpp cimport bool

cdef extern from "taos.h":
    ctypedef void TAOS
    ctypedef void TAOS_STMT
    ctypedef void TAOS_RES
    ctypedef void **TAOS_ROW
    ctypedef struct TAOS_FIELD:
        char name[65]
        int8_t type
        int32_t bytes
    ctypedef enum TSDB_OPTION:
        TSDB_OPTION_LOCALE
        TSDB_OPTION_CHARSET
        TSDB_OPTION_TIMEZONE
        TSDB_OPTION_CONFIGDIR
        TSDB_OPTION_SHELL_ACTIVITY_TIMER
        TSDB_OPTION_USE_ADAPTER
        TSDB_MAX_OPTIONS
    ctypedef struct TAOS_MULTI_BIND:
        int       buffer_type
        void     *buffer
        uintptr_t buffer_length
        int32_t  *length
        char     *is_null
        int       num

    ctypedef void (*__taos_async_fn_t)(void *param, TAOS_RES *res, int num_of_rows) except *
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
    int taos_options(TSDB_OPTION option, const void *arg, ...)
    const char *taos_get_client_info()
    const char *taos_get_server_info(TAOS *taos)
    int taos_select_db(TAOS *taos, const char *db)
    TAOS_RES *taos_query(TAOS *taos, const char *sql)
    TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqId)
    int taos_affected_rows(TAOS_RES *res)
    void taos_free_result(TAOS_RES *res)
    TAOS_ROW taos_fetch_row(TAOS_RES *res)
    void taos_fetch_rows_a(TAOS_RES *res, __taos_async_fn_t fp, void *param)
    void taos_query_a(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param)
    void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid)
    void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param)
    const void *taos_get_raw_block(TAOS_RES *res)
    TAOS_STMT *taos_stmt_init(TAOS *taos)
    int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)
    int taos_stmt_set_tbname(TAOS_STMT *stmt, const char *name)
    int taos_stmt_set_tbname_tags(TAOS_STMT *stmt, const char *name, TAOS_MULTI_BIND *tags)
    int taos_stmt_affected_rows(TAOS_STMT *stmt)
    TAOS_RES *taos_stmt_use_result(TAOS_STMT *stmt)
    int taos_stmt_execute(TAOS_STMT *stmt)
    int taos_stmt_add_batch(TAOS_STMT *stmt)
    int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
    int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
    int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId)
    int taos_load_table_info(TAOS *taos, const char *tableNameList)

cdef list taos_get_column_data_is_null(TAOS_RES *res, int field, int rows)
cdef taos_fetch_block_v3(TAOS_RES *res, TAOS_FIELD *fields, int field_count, dt_epoch)
