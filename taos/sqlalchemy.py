from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection

TYPES_MAP = {
    "bool": sqltypes.Boolean,
    "timestamp": sqltypes.TIMESTAMP,
    "tinyint": sqltypes.SmallInteger,
    "smallint": sqltypes.SmallInteger,
    "int": sqltypes.Integer,
    "bigint": sqltypes.BigInteger,
    "tinyint unsigned": sqltypes.SmallInteger,
    "smallint unsigned": sqltypes.SmallInteger,
    "int unsigned": sqltypes.Integer,
    "bigint unsigned": sqltypes.BigInteger,
    "float": sqltypes.FLOAT,
    "double": sqltypes.DECIMAL,
    "nchar": sqltypes.String,
    "binary": sqltypes.String,
}


class TaosDialect(default.DefaultDialect):
    name = "taos"
    driver = "taos"
    supports_native_boolean = True
    implicit_returning = True

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        return tuple(connection.connection.server_info)

    @classmethod
    def dbapi(cls):
        import taos

        return taos

    def has_schema(self, connection, schema):
        return False

    def has_table(self, connection, table_name, schema=None):
        try:
            cursor = connection.execute("describe {}" % table_name)
            True
        except:
            False

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Gets all indexes
        """
        # no index is supported by TDengine
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            cursor = connection.execute("describe {}" % table_name)
            return [row[0] for row in cursor.fetchall()]
        except:
            return []

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)
