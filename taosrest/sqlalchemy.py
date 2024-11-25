from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection

import taosrest

TYPES_MAP = {
    "TIMESTAMP": sqltypes.DATETIME,
    "INT": sqltypes.Integer,
    "INT UNSIGNED": sqltypes.Integer,
    "BIGINT": sqltypes.BigInteger,
    "BIGINT UNSIGNED": sqltypes.BigInteger,
    "FLOAT": sqltypes.FLOAT,
    "DOUBLE": sqltypes.DECIMAL,
    "BINARY": sqltypes.String,
    "SMALLINT": sqltypes.SmallInteger,
    "SMALLINT UNSIGNED": sqltypes.SmallInteger,
    "TINYINT": sqltypes.SmallInteger,
    "TINYINT UNSIGNED": sqltypes.SmallInteger,
    "BOOL": sqltypes.Boolean,
    "NCHAR": sqltypes.String,
    "JSON": sqltypes.JSON,
    "VARCHAR": sqltypes.String,
    "VARBINARY": sqltypes.String,
}


class AlchemyRestConnection:
    threadsafety = 0
    paramstyle = "pyformat"
    Error = taosrest.Error

    def connect(self, **kwargs):
        host = kwargs["host"] if "host" in kwargs else None
        port = kwargs["port"] if "port" in kwargs else None
        user = kwargs["username"] if "username" in kwargs else "root"
        password = kwargs["password"] if "password" in kwargs else "taosdata"
        database = kwargs["database"] if "database" in kwargs else None
        token = kwargs["token"] if "token" in kwargs else None

        if not host:
            host = 'localhost'
        if host == 'localhost' and not port:
            port = 6041

        url = f"http://{host}"
        if port:
            url += f':{port}'
        return taosrest.connect(url=url, user=user, password=password, database=database, token=token)


class TaosRestDialect(default.DefaultDialect):
    name = "taosrest"
    driver = "taosrest"
    supports_native_boolean = True
    implicit_returning = True
    supports_statement_cache = True

    def do_rollback(self, connection):
        pass

    def _get_server_version_info(self, connection):
        return tuple(connection.connection.server_info)

    @classmethod
    def dbapi(cls):
        return AlchemyRestConnection()

    @classmethod
    def import_dbapi(cls):
        return AlchemyRestConnection()
    
    @reflection.cache
    def get_schema_names(self, connection, **kw):
        cursor = connection.execute("SHOW databases")
        return [row[0] for row in cursor.fetchall()]

    @reflection.cache
    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)
        
    @reflection.cache   
    def get_table_names(self, connection, schema=None, **kw):
        sql = (
            "SELECT stable_name FROM information_schema.INS_STABLES "
            f"WHERE db_name = '{schema}'"
        )
        cursor = connection.execute(sql)
        return [row[0] for row in cursor.fetchall()]
        
    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        # view is only supported by TDengine Enterprise Edition
        return []

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        cursor = connection.execute(f"DESCRIBE {schema}.{table_name}")
        columns = []
        
        for row in cursor.fetchall():
            column = dict(row)
            column["name"] = column.pop("field")
            column["type"] = self._resolve_type(column["type"])
            columns.append(column)
            
        return columns
        
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
        
        cursor = connection.execute(sql)
        rows = cursor.fetchall()
        indexes = []
        
        for row in rows:
            indexes.append(
                {"name": row[0], "column_names": [row[5]], "type": "index", "unique": False}
            )
        
        return indexes
        
    def _resolve_type(self, type_):
        return TYPES_MAP.get(type_, sqltypes.UserDefinedType)

