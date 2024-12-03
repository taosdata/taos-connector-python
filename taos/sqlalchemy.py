import sys
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
    "varchar": sqltypes.String,
    "varbinary": sqltypes.String,
}


class TaosWsDialect(default.DefaultDialect):
    name = "taosws"
    driver = "taosws"
    supports_native_boolean = True
    implicit_returning = True
    supports_statement_cache = True

    def do_rollback(self, connection):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        pass

    def _get_server_version_info(self, connection):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        cursor = connection.execute(text("select server_version()"))
        return cursor.fetchone()

    @classmethod
    def dbapi(cls):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        import taosws

        return taosws

    @classmethod
    def import_dbapi(cls):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        import taosws
        return taosws


    def has_schema(self, connection, schema):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        return True

    def has_table(self, connection, table_name, schema=None):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        return table_name in self.get_table_names(connection, schema)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        # no index is supported by TDengine
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        if schema is None:
            sql = f"describe {table_name}"
        else:
            sql = f"describe {schema}.{table_name}"
        try:
            cursor = connection.execute(sql)
            columns = []
            for row in cursor.fetchall():
                column = dict(row)
                column["name"] = column.pop("field")
                column["type"] = self._resolve_type(column["type"])
                columns.append(column)
            print(columns)    
            return columns
        except:
            return []

    @reflection.cache    
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        columns = self.get_columns(connection, table_name, schema)
        return {"constrained_columns": [columns[0]["name"]], "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # no foreign key is supported by TDengine
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        sql = (
            "SELECT * FROM information_schema.INS_INDEXES "
            f"WHERE db_name = '{schema}'"
            f"AND table_name = '{table_name}'"
        )

        cursor = connection.execute(sql)
        rows = cursor.fetchall()
        indexes = []

        for row in rows:
            indexes.append(
                {"name": row[0], "column_names": [row[5]], "type": "index", "unique": False}
            )

        return indexes


    @reflection.cache
    def get_schema_names(self, connection, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        sql = "select name from information_schema.ins_databases"
        try:
            cursor = connection.execute(sql)
            return [row[0] for row in cursor.fetchall()]
        except:
            return []
    
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        print(f"call function {sys._getframe().f_code.co_name} ... \n")
        sql = f"select stable_name from information_schema.ins_stables where db_name = '{schema}'"
        try:
            cursor = connection.execute(sql)
            tables = [row[0] for row in cursor.fetchall()]
            print(f"sql={sql} tables={tables}")
            return tables
        except:
            return []

    def _resolve_type(self, type_):
        print(f"call function {sys._getframe().f_code.co_name} type: {type_} ...\n")
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)


class AlchemyTaosConnection:
    paramstyle = "pyformat"

    def connect(self, **kwargs):
        host = kwargs.get("host", "localhost")
        port = kwargs.get("port", "6030")
        user = kwargs.get("username", "root")
        password = kwargs.get("password", "taosdata")
        database = kwargs.get("database", None)

        import taos

        return taos.connect(host=host, user=user, password=password, port=int(port), database=database)


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
