from sqlalchemy import types as sql_types
from sqlalchemy.engine import default, reflection

TYPES_MAP = {
    "bool": sql_types.Boolean,
    "timestamp": sql_types.TIMESTAMP,
    "tinyint": sql_types.SmallInteger,
    "smallint": sql_types.SmallInteger,
    "int": sql_types.Integer,
    "bigint": sql_types.BigInteger,
    "tinyint unsigned": sql_types.SmallInteger,
    "smallint unsigned": sql_types.SmallInteger,
    "int unsigned": sql_types.Integer,
    "bigint unsigned": sql_types.BigInteger,
    "float": sql_types.FLOAT,
    "double": sql_types.DECIMAL,
    "nchar": sql_types.String,
    "binary": sql_types.String,
}


class TaosRestDialect(default.DefaultDialect):
    name = "taosrest"
    driver = "taosrest"
    supports_native_boolean = True
    implicit_returning = True

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        return tuple(connection.connection.server_info)

    @classmethod
    def dbapi(cls):
        import taosrest

        return taosrest

    def has_schema(self, connection, schema):
        return False

    def has_table(self, connection, table_name, schema=None):
        try:
            connection.cursor().execute(f"describe {table_name}")
            return True
        except:
            return False

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Gets all indexes
        """
        # no index is supported by TDengine
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            cursor = connection.cursor()
            cursor.execute("describe {}" % table_name)
            return [row[0] for row in cursor.fetchall()]
        except:
            return []

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, sql_types.UserDefinedType)
