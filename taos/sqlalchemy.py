from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import text

TYPES_MAP = {
    "bool": sqltypes.Boolean,
    "timestamp": sqltypes.DATETIME,
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


class TaosWsDialect(default.DefaultDialect):
    name = "taosws"
    driver = "taosws"
    supports_native_boolean = True
    implicit_returning = True
    supports_statement_cache = True

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        cursor = connection.execute(text("select server_version()"))
        return cursor.fetchone()

    @classmethod
    def dbapi(cls):
        import taosws

        return taosws

    @classmethod
    def import_dbapi(cls):
        import taosws
        return taosws

    def has_schema(self, connection, schema):
        return False

    def has_table(self, connection, table_name, schema=None):
        try:
            connection.execute(text("describe {}" % table_name))
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
            cursor = connection.execute(text("describe {}" % table_name))
            return [row[0] for row in cursor.fetchall()]
        except:
            return []

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)


class AlchemyTaosConnection:
    paramstyle = "pyformat"

    def connect(self, **kwargs):
        host = kwargs["host"] if "host" in kwargs else "localhost"
        port = kwargs["port"] if "port" in kwargs else "6030"
        user = kwargs["username"] if "username" in kwargs else "root"
        password = kwargs["password"] if "password" in kwargs else "taosdata"

        import taos

        return taos.connect(host=host, user=user, password=password, port=int(port))


class TaosDialect(default.DefaultDialect):
    name = "taos"
    driver = "taos"
    supports_native_boolean = True
    implicit_returning = True
    supports_statement_cache = True

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        return tuple(connection.connection.server_info)

    @classmethod
    def dbapi(cls):
        return AlchemyTaosConnection()

    @classmethod
    def import_dbapi(cls):
        return AlchemyTaosConnection()

    def has_schema(self, connection, schema):
        return False

    def has_table(self, connection, table_name, schema=None):
        try:
            connection.execute(text("describe {}" % table_name))
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
            cursor = connection.execute(text("describe {}" % table_name))
            return [row[0] for row in cursor.fetchall()]
        except:
            return []

    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)
