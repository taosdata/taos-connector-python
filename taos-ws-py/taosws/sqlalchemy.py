import numbers
from datetime import date, datetime

from sqlalchemy import sql
from sqlalchemy import text
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from urllib.parse import quote, urlencode

TYPES_MAP = {
    "BOOL": sqltypes.Boolean,
    "TIMESTAMP": sqltypes.DATETIME,
    "INT": sqltypes.Integer,
    "INT UNSIGNED": sqltypes.Integer,
    "BIGINT": sqltypes.BigInteger,
    "BIGINT UNSIGNED": sqltypes.BigInteger,
    "FLOAT": sqltypes.FLOAT,
    "DOUBLE": sqltypes.FLOAT,
    "TINYINT": sqltypes.SmallInteger,
    "TINYINT UNSIGNED": sqltypes.SmallInteger,
    "SMALLINT": sqltypes.SmallInteger,
    "SMALLINT UNSIGNED": sqltypes.SmallInteger,
    "BINARY": sqltypes.String,
    "VARCHAR": sqltypes.String,
    "VARBINARY": sqltypes.BINARY,
    "NCHAR": sqltypes.Unicode,
    "JSON": sqltypes.JSON,
    "BLOB": sqltypes.BLOB,
    "GEOMETRY": sqltypes.BINARY,
}

# TDengine reserved words
RESERVED_WORDS_TDENGINE = {
    "account",
    "accounts",
    "add",
    "aggregate",
    "all",
    "alter",
    "analyze",
    "and",
    "anti",
    "anode",
    "anodes",
    "anomaly_window",
    "apps",
    "as",
    "asc",
    "asof",
    "at_once",
    "balance",
    "batch_scan",
    "between",
    "bigint",
    "binary",
    "bnode",
    "bnodes",
    "bool",
    "both",
    "buffer",
    "bufsize",
    "by",
    "cache",
    "cachemodel",
    "cachesize",
    "case",
    "cast",
    "child",
    "client_version",
    "cluster",
    "column",
    "comment",
    "comp",
    "compact",
    "compacts",
    "connection",
    "connections",
    "conns",
    "consumer",
    "consumers",
    "contains",
    "count",
    "count_window",
    "create",
    "createdb",
    "current_user",
    "database",
    "databases",
    "dbs",
    "decimal",
    "delete",
    "delete_mark",
    "desc",
    "describe",
    "distinct",
    "distributed",
    "dnode",
    "dnodes",
    "double",
    "drop",
    "duration",
    "else",
    "enable",
    "encryptions",
    "encrypt_algorithm",
    "encrypt_key",
    "end",
    "exists",
    "expired",
    "explain",
    "event_window",
    "every",
    "file",
    "fill",
    "fill_history",
    "first",
    "float",
    "flush",
    "from",
    "for",
    "force",
    "full",
    "function",
    "functions",
    "geometry",
    "grant",
    "grants",
    "full",
    "logs",
    "machines",
    "group",
    "hash_join",
    "having",
    "host",
    "if",
    "ignore",
    "import",
    "in",
    "index",
    "indexes",
    "inner",
    "insert",
    "int",
    "integer",
    "interval",
    "into",
    "is",
    "jlimit",
    "join",
    "json",
    "keep",
    "key",
    "kill",
    "language",
    "last",
    "last_row",
    "leader",
    "leading",
    "left",
    "licences",
    "like",
    "limit",
    "linear",
    "local",
    "match",
    "maxrows",
    "max_delay",
    "bwlimit",
    "merge",
    "meta",
    "only",
    "minrows",
    "minus",
    "mnode",
    "mnodes",
    "modify",
    "modules",
    "normal",
    "nchar",
    "next",
    "near",
    "nmatch",
    "none",
    "not",
    "now",
    "no_batch_scan",
    "null",
    "null_f",
    "nulls",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "outputtype",
    "pages",
    "pagesize",
    "para_tables_sort",
    "partition",
    "partition_first",
    "pass",
    "port",
    "position",
    "pps",
    "primary",
    "precision",
    "prev",
    "privileges",
    "qnode",
    "qnodes",
    "qtime",
    "queries",
    "query",
    "pi",
    "rand",
    "range",
    "ratio",
    "pause",
    "read",
    "recursive",
    "redistribute",
    "rename",
    "replace",
    "replica",
    "reset",
    "resume",
    "restore",
    "retentions",
    "revoke",
    "right",
    "rollup",
    "schemaless",
    "scores",
    "select",
    "semi",
    "server_status",
    "server_version",
    "session",
    "set",
    "show",
    "single_stable",
    "skip_tsma",
    "sliding",
    "slimit",
    "sma",
    "smalldata_ts_sort",
    "smallint",
    "snode",
    "snodes",
    "sort_for_group",
    "soffset",
    "split",
    "stable",
    "stables",
    "start",
    "state",
    "state_window",
    "storage",
    "stream",
    "streams",
    "strict",
    "stt_trigger",
    "subscribe",
    "subscriptions",
    "substr",
    "substring",
    "subtable",
    "sysinfo",
    "system",
    "table",
    "tables",
    "table_prefix",
    "table_suffix",
    "tag",
    "tags",
    "tbname",
    "then",
    "timestamp",
    "timezone",
    "tinyint",
    "to",
    "today",
    "topic",
    "topics",
    "trailing",
    "transaction",
    "transactions",
    "trigger",
    "trim",
    "tsdb_pagesize",
    "tseries",
    "tsma",
    "tsmas",
    "ttl",
    "union",
    "unsafe",
    "unsigned",
    "untreated",
    "update",
    "use",
    "user",
    "users",
    "using",
    "value",
    "value_f",
    "values",
    "varchar",
    "variables",
    "verbose",
    "vgroup",
    "vgroups",
    "view",
    "views",
    "vnode",
    "vnodes",
    "wal_fsync_period",
    "wal_level",
    "wal_retention_period",
    "wal_retention_size",
    "wal_roll_period",
    "wal_segment_size",
    "watermark",
    "when",
    "where",
    "window",
    "window_close",
    "window_offset",
    "with",
    "write",
    "_c0",
    "_irowts",
    "_irowts_origin",
    "_isfilled",
    "_qduration",
    "_qend",
    "_qstart",
    "_rowts",
    "_tags",
    "_wduration",
    "_wend",
    "_wstart",
    "_flow",
    "_fhigh",
    "_frowts",
    "alive",
    "varbinary",
    "s3_chunkpages",
    "s3_keeplocal",
    "s3_compact",
    "s3migrate",
    "keep_time_offset",
    "arbgroups",
    "is_import",
    "force_window_close",
}

# NOTE: reserved words are synced from TDengine parser keyword table when needed.


#
# identifier for TDengine
#
class TDengineIdentifierPreparer(sql.compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS_TDENGINE

    def __init__(self, dialect, server_ansiquotes=False, **kw):
        if not server_ansiquotes:
            quote = "`"
        else:
            quote = '"'

        super(TDengineIdentifierPreparer, self).__init__(dialect, initial_quote=quote, escape_quote=quote)

    def _quote_free_identifiers(self, *ids):
        """Unilaterally identifier-quote any number of strings."""
        return tuple([self.quote_identifier(i) for i in ids if i is not None])


#
# base class for dialect
#
class BaseDialect(default.DefaultDialect):
    supports_native_boolean = True
    implicit_returning = True
    # supports_statement_cache = True

    # Set back-quote identifier preparer for TDengine keywords
    preparer = TDengineIdentifierPreparer

    def is_sys_db(self, dbname):
        return dbname.lower() in ["information_schema", "performance_schema"]

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        cursor = connection.execute(text("select server_version()"))
        return cursor.fetchone()

    def _to_sql_literal(self, value):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, numbers.Number):
            return str(value)
        if isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        if isinstance(value, date):
            return f"'{value.isoformat()}'"

        text_value = str(value)
        return "'" + text_value.replace("'", "''") + "'"

    def _render_sql(self, statement, parameters):
        if parameters is None:
            return statement

        if isinstance(parameters, dict):
            if len(parameters) == 0:
                return statement
            literalized = {key: self._to_sql_literal(value) for key, value in parameters.items()}
            return statement % literalized

        if isinstance(parameters, (list, tuple)):
            if len(parameters) == 0:
                return statement
            literalized = tuple(self._to_sql_literal(value) for value in parameters)
            return statement % literalized

        return statement % self._to_sql_literal(parameters)

    def do_execute(self, cursor, statement, parameters, context=None):
        rendered_sql = self._render_sql(statement, parameters)
        cursor.execute(rendered_sql)

        return cursor

    def do_executemany(self, cursor, statement, parameters, context=None):
        if parameters is None:
            return cursor
        if isinstance(parameters, (list, tuple)) and len(parameters) == 0:
            return cursor

        for parameter in parameters:
            rendered_sql = self._render_sql(statement, parameter)
            cursor.execute(rendered_sql)
        return cursor

    @reflection.cache
    def has_schema(self, connection, schema, **kw):
        return schema in self.get_schema_names(connection)

    # Check if table exists
    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        return table_name in self.get_table_names(connection, schema)

    # Get column information
    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        sysdb = False
        if schema is None:
            sql = f"describe {table_name}"
        else:
            sql = f"describe {schema}.{table_name}"
            # sysdb = self.is_sys_db(schema)
        try:
            cursor = connection.execute(text(sql))
            columns = []
            for row in cursor.fetchall():
                # print(row)
                column = dict()
                column["name"] = row[0]
                column["type"] = self._resolve_type(row[1])
                columns.append(column)
            return columns
        except Exception:
            return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        columns = self.get_columns(connection, table_name, schema)
        if not columns:
            return {"constrained_columns": [], "name": None}
        return {"constrained_columns": [columns[0]["name"]], "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No foreign key is supported by TDengine
        return []

    # Get indexes information
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        if schema is None:
            return []

        sql = text(
            "SELECT * FROM information_schema.INS_INDEXES " "WHERE db_name = :schema " "AND table_name = :table_name"
        )
        try:
            cursor = connection.execute(sql, {"schema": schema, "table_name": table_name})
            rows = cursor.fetchall()
            indexes = []
            for row in rows:
                index = {"name": row[0], "column_names": [row[5]], "type": "index", "unique": False}
                indexes.append(index)
            return indexes
        except Exception:
            return []

    # Get database names
    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sql = text("SHOW DATABASES")
        try:
            cursor = connection.execute(sql)
            names = []
            for row in cursor.fetchall():
                if self.is_sys_db(row[0]) is False:
                    names.append(row[0])
            return names
        except:
            return []

    # Get table names
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            sqls = [f"show stables", f"show normal tables"]
        else:
            sqls = [f"show `{schema}`.stables", f"show normal `{schema}`.tables"]
        # Execute queries
        try:
            names = []
            for sql in sqls:
                cursor = connection.execute(text(sql))
                for row in cursor.fetchall():
                    names.append(row[0])
            return names
        except:
            return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is None:
            return []
        # SQL query for views
        sql = f"show `{schema}`.views"
        # Execute query
        try:
            cursor = connection.execute(text(sql))
            return [row[0] for row in cursor.fetchall()]
        except:
            return []

    def _resolve_type(self, type_):
        # print(f"call function {sys._getframe().f_code.co_name} type: {type_} ...\n")
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)


# WebSocket dialect
class TaosWsDialect(BaseDialect):
    name = "taosws"
    driver = "taosws"

    @classmethod
    def dbapi(cls):
        import taosws

        return taosws

    @classmethod
    def import_dbapi(cls):
        import taosws

        return taosws

    @classmethod
    def create_connect_args(cls, url):
        username = url.username
        password = url.password

        has_username = username is not None and username != ""
        has_password = password is not None and password != ""

        if has_username and has_password:
            userpass = f"{username}:{password}"
        elif has_username:
            userpass = f"{username}"
        elif has_password:
            userpass = f":{password}"
        else:
            userpass = ""

        at = "@" if (has_username or has_password) else ""

        hosts = url.query.get("hosts")
        if hosts:
            addr = hosts
        else:
            if url.host and url.port:
                addr = f"{url.host}:{url.port}"
            elif url.host:
                addr = f"{url.host}"
            elif url.port:
                addr = f":{url.port}"
            else:
                addr = ""

        query_params = [(key, value) for key, value in url.query.items() if key != "hosts"]
        params = urlencode(query_params, doseq=True, quote_via=quote)

        dsn = f"{url.drivername}://{userpass}{at}{addr}"
        if url.database:
            dsn += f"/{url.database}"
        if params:
            dsn += f"?{params}"

        return ([dsn], {})
