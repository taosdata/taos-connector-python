from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, uintptr_t

ctypedef bint bool

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
    ctypedef enum TSDB_SML_PROTOCOL_TYPE:
        TSDB_SML_UNKNOWN_PROTOCOL
        TSDB_SML_LINE_PROTOCOL 
        TSDB_SML_TELNET_PROTOCOL
        TSDB_SML_JSON_PROTOCOL
    ctypedef enum TSDB_SML_TIMESTAMP_TYPE:
        TSDB_SML_TIMESTAMP_NOT_CONFIGURED
        TSDB_SML_TIMESTAMP_HOURS
        TSDB_SML_TIMESTAMP_MINUTES
        TSDB_SML_TIMESTAMP_SECONDS
        TSDB_SML_TIMESTAMP_MILLI_SECONDS
        TSDB_SML_TIMESTAMP_MICRO_SECONDS
        TSDB_SML_TIMESTAMP_NANO_SECONDS
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
    void taos_stop_query(TAOS_RES *res)
    int taos_field_count(TAOS_RES *res)
    int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
    int taos_result_precision(TAOS_RES *res)
    int *taos_get_column_data_offset(TAOS_RES *res, int columnIndex)
    int taos_validate_sql(TAOS *taos, const char *sql)
    int taos_errno(TAOS_RES *res)
    char *taos_errstr(TAOS_RES *res)
    TAOS *taos_connect(const char *ip, const char *user, const char *password, const char *db, uint16_t port)
    void taos_close(TAOS *taos)
    int taos_options(TSDB_OPTION option, const void *arg, ...)
    const char *taos_get_client_info()
    const char *taos_get_server_info(TAOS *taos)
    int taos_get_current_db(TAOS *taos, char *database, int len, int *required)
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
    int taos_stmt_close(TAOS_STMT *stmt)
    int taos_stmt_execute(TAOS_STMT *stmt)
    int taos_stmt_add_batch(TAOS_STMT *stmt)
    int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
    int taos_stmt_bind_param_batch(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)
    int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId)
    int taos_load_table_info(TAOS *taos, const char *tableNameList)
    char *taos_stmt_errstr(TAOS_STMT *stmt)

    TAOS_RES *taos_schemaless_insert(TAOS *taos, char *lines[], int numLines, int protocol, int precision)
    TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid)
    TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision)
    TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid)
    TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl)
    TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid)
    TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl)
    TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)
    ctypedef struct tmq_t:
        pass
    ctypedef struct tmq_conf_t:
        pass
    ctypedef struct tmq_list_t:
        pass
    ctypedef void tmq_commit_cb(tmq_t *tmq, int32_t code, void *param) except *
    ctypedef enum tmq_conf_res_t:
        TMQ_CONF_UNKNOWN
        TMQ_CONF_INVALID
        TMQ_CONF_OK

    ctypedef struct tmq_topic_assignment:
        int32_t vgId
        int64_t currentOffset
        int64_t begin
        int64_t end

    tmq_conf_t *tmq_conf_new()
    tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value)
    void tmq_conf_destroy(tmq_conf_t *conf)
    void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param)

    tmq_list_t *tmq_list_new()
    int32_t tmq_list_append(tmq_list_t *, const char *)
    void tmq_list_destroy(tmq_list_t *)
    int32_t tmq_list_get_size(const tmq_list_t *)
    char **tmq_list_to_c_array(const tmq_list_t *)

    tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen)
    int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list)
    int32_t tmq_unsubscribe(tmq_t *tmq)
    int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics)
    TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout)
    int32_t tmq_consumer_close(tmq_t *tmq)
    int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg)
    int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset)
    void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param)
    void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param)
    int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment,int32_t *numOfAssignment)
    void tmq_free_assignment(tmq_topic_assignment* pAssignment)
    int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset)
    int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId)
    int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId)

    const char *tmq_get_topic_name(TAOS_RES *res)
    const char *tmq_get_db_name(TAOS_RES *res)
    int32_t tmq_get_vgroup_id(TAOS_RES *res)
    int64_t tmq_get_vgroup_offset(TAOS_RES* res)
    const char *tmq_err2str(int32_t code)

    ctypedef enum tmq_res_t:
        TMQ_RES_INVALID
        TMQ_RES_DATA
        TMQ_RES_TABLE_META
        TMQ_RES_METADATA

    const char *tmq_get_table_name(TAOS_RES *res)
    tmq_res_t   tmq_get_res_type(TAOS_RES *res)

cdef bool *taos_get_column_data_is_null(TAOS_RES *res, int field, int rows)
cdef taos_fetch_block_v3(TAOS_RES *res, TAOS_FIELD *fields, int field_count, object dt_epoch)
