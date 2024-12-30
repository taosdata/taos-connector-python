import sys
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import text
from sqlalchemy import sql

TYPES_MAP = {
    "BOOL"             : sqltypes.Boolean,
    "TIMESTAMP"        : sqltypes.DATETIME,
    "INT"              : sqltypes.Integer,
    "INT UNSIGNED"     : sqltypes.Integer,
    "BIGINT"           : sqltypes.BigInteger,
    "BIGINT UNSIGNED"  : sqltypes.BigInteger,
    "FLOAT"            : sqltypes.FLOAT,
    "DOUBLE"           : sqltypes.FLOAT,
    "TINYINT"          : sqltypes.SmallInteger,
    "TINYINT UNSIGNED" : sqltypes.SmallInteger,
    "SMALLINT"         : sqltypes.SmallInteger,
    "SMALLINT UNSIGNED": sqltypes.SmallInteger,
    "BINARY"           : sqltypes.String,
    "VARCHAR"          : sqltypes.String,
    "VARBINARY"        : sqltypes.String,
    "NCHAR"            : sqltypes.Unicode,
    "JSON"             : sqltypes.JSON,
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
    "force_window_close"
}

# backup generator function
'''
generator from TDengine/source/libs/parse/src/parTokenizer.c -> keywordTable

import sys
def readKeyWord(filename):
    keys = ""
    print(f"read file {filename}\n")
    with open(filename) as file:
        for line in file.readlines():
            pos1 = line.find('"')
            if pos1 == -1 :
                print(f"NO FOUND FIRST QUOTA: {line}\n")
                continue
            pos2 = line.find('"', pos1 + 1)
            if pos2 == -1 :
                print(f"NO FOUND SECOND QUOTA: {line}\n")
                continue
            word = line[pos1:pos2+1]
            if keys == "":
                keys = "RESERVED_WORDS_TDENGINE = {\n    " + word.lower()
            else:
                keys += ",\n    " + word.lower()

    # end
    keys += "\n}"
    print(f"\n\n{keys}\n")


if __name__ == "__main__":
    readKeyWord("./keyword.txt")

'''

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

        super(TDengineIdentifierPreparer, self).__init__(
            dialect, initial_quote=quote, escape_quote=quote
        )

    def _quote_free_identifiers(self, *ids):
        """Unilaterally identifier-quote any number of strings."""
        return tuple([self.quote_identifier(i) for i in ids if i is not None])

#
#  base class for dialect
#
class BaseDialect(default.DefaultDialect):
    supports_native_boolean = True
    implicit_returning = True
    #supports_statement_cache = True

    # set back-quote and time grain keywords
    preparer = TDengineIdentifierPreparer

    def is_sys_db(self, dbname):
        return dbname.lower() in [ "information_schema", "performance_schema"]

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        cursor = connection.execute(text("select server_version()"))
        return cursor.fetchone()

    @reflection.cache
    def has_schema(self, connection, schema):
        return schema in self.get_schema_names(connection)

    # has table
    @reflection.cache
    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    # get column
    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        sysdb = False
        if schema is None:
            sql = f"describe {table_name}"
        else:
            sql = f"describe {schema}.{table_name}"
            sysdb = self.is_sys_db(schema)
        try:
            cursor = connection.execute(sql)
            columns = []
            for row in cursor.fetchall():
                #print(row)
                column = dict()
                column["name"] = row[0]
                column["type"] = self._resolve_type(row[1])
                columns.append(column)
            return columns
        except:
            return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):        
        columns = self.get_columns(connection, table_name, schema)
        return {"constrained_columns": [columns[0]["name"]], "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # no foreign key is supported by TDengine
        return []

    # get indexs
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        sql = (
            "SELECT * FROM information_schema.INS_INDEXES "
            f"WHERE db_name = '{schema}'"
            f"AND table_name = '{table_name}'"
        )
        try:
            cursor = connection.execute(sql)
            rows = cursor.fetchall()
            indexes = []
            for row in rows:
                index = {"name": row[0], "column_names": [row[5]], "type": "index", "unique": False}
                indexes.append(index)
            return indexes
        except:
            return []

    # get database name
    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sql = "show databases"
        try:
            cursor = connection.execute(sql)
            names = []
            for row in cursor.fetchall():
                if self.is_sys_db(row[0]) is False:
                    names.append(row[0])
            return names
        except:
            return []
    
    # get table names
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            return []
        # sql
        sqls = [
            f"show `{schema}`.stables",
            f"show normal `{schema}`.tables"]
        # execute
        try:
            names = []
            for sql in sqls:
                cursor = connection.execute(sql)
                for row in cursor.fetchall():
                    names.append(row[0])
            return names
        except:
            return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is None:
            return []
        # sql        
        sql =  f"show `{schema}`.views"
        # execute
        try:
            
            cursor = connection.execute(sql)
            return [row[0] for row in cursor.fetchall() ]
        except:
            return []

    def _resolve_type(self, type_):
        #print(f"call function {sys._getframe().f_code.co_name} type: {type_} ...\n")
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)


#
# ---------------- taos impl -------------
#
import taos

#
# Alchemy connect
#
class AlchemyTaosConnection:
    paramstyle = "pyformat"
    # connect
    def connect(self, **kwargs):
        host = kwargs.get("host", "localhost")
        port = kwargs.get("port", "6030")
        user = kwargs.get("username", "root")
        password = kwargs.get("password", "taosdata")
        database = kwargs.get("database", None)
        return taos.connect(host=host, user=user, password=password, port=int(port), database=database)

# taos dialet
class TaosDialect(BaseDialect):
    name = "taos"
    driver = "taos"

    @classmethod
    def dbapi(cls):
        return AlchemyTaosConnection()

    @classmethod
    def import_dbapi(cls):
        return AlchemyTaosConnection()


#
# ---------------- taosws impl -------------
#

# ws dailet
class TaosWsDialect(BaseDialect):
    # set taosws
    name = "taosws"
    driver = "taosws"

    # doapi
    @classmethod
    def dbapi(cls):
        import taosws
        return taosws

    # import dbapi
    @classmethod
    def import_dbapi(cls):
        import taosws
        return taosws
