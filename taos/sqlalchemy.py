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


#
#  base class for dialect
#
class BaseDialect(default.DefaultDialect):
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
        sql = "select name from information_schema.ins_databases where `vgroups` is not null"
        try:
            cursor = connection.execute(sql)
            return [row[0] for row in cursor.fetchall()]
        except:
            return []
    
    # get table names
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            return []
        # sql
        sqls = [
            f"select stable_name from information_schema.ins_stables where db_name = '{schema}'",
            f"select  table_name from information_schema.ins_tables  where db_name = '{schema}' and type='NORMAL_TABLE'"]
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
import taosws

# ws dailet
class TaosWsDialect(BaseDialect):
    # set taosws
    name = "taosws"
    driver = "taosws"

    # doapi
    @classmethod
    def dbapi(cls):
        return taosws

    # import dbapi
    @classmethod
    def import_dbapi(cls):
        return taosws
