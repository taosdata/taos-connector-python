from sqlalchemy.engine import default, reflection
from sqlalchemy import text

import taosrest


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

    def has_schema(self, connection, schema):
        return False

    def has_table(self, connection, table_name, schema=None):
        try:
            connection.execute(text(f"describe {table_name}"))
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
