import sys
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import text

TYPES_MAP = {
    "BOOL"             : sqltypes.Boolean,
    "TIMESTAMP"        : sqltypes.DATETIME,
    "INT"              : sqltypes.Integer,
    "INT UNSIGNED"     : sqltypes.Integer,
    "BIGINT"           : sqltypes.BigInteger,
    "BIGINT UNSIGNED"  : sqltypes.BigInteger,
    "FLOAT"            : sqltypes.FLOAT,
    "DOUBLE"           : sqltypes.DECIMAL,
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


class TaosWsDialect(default.DefaultDialect):
    name = "taosws"
    driver = "taosws"
    supports_native_boolean = True
    implicit_returning = True
    supports_statement_cache = True

    def is_sys_db(self, dbname):
        return dbname.lower() in [ "information_schema", "performance_schema"]

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


    @reflection.cache
    def has_schema(self, connection, schema):
        
        return True

    @reflection.cache
    def has_table(self, connection, table_name, schema=None):
        
        return table_name in self.get_table_names(connection, schema)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        
        # no index is supported by TDengine
        return []

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
                if sysdb:
                    #column["name"] = "`" + row[0] + "`"
                    column["name"] = row[0]
                else:
                    column["name"] = row[0]
                column["type"] = self._resolve_type(row[1])
                #print(column)
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


    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sql = "select name from information_schema.ins_databases"
        try:
            cursor = connection.execute(sql)
            return [row[0] for row in cursor.fetchall()]
        except:
            return []
    
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        
        if schema is None:
            return []
        if schema.lower() in [ "information_schema", "performance_schema"]:
            sql = f"select table_name from information_schema.ins_tables where db_name = '{schema}'"
        else:
            sql = f"select stable_name from information_schema.ins_stables where db_name = '{schema}'"
        try:

            cursor = connection.execute(sql)
            names = [row[0] for row in cursor.fetchall()]
            print(f"sql={sql} names={names}\n")
            return names
        except:
            return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        return []

    def _resolve_type(self, type_):
        #print(f"call function {sys._getframe().f_code.co_name} type: {type_} ...\n")
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
            connection.execute(f"describe {table_name}")
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
        return TYPES_MAP.get(type_.lower(), sqltypes.UserDefinedType)
