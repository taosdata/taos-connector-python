# encoding:UTF-8
import os
from taos.bind import *
from taos.bind2 import *
from taos.connection import TaosConnection
from taos.cursor import *

# For some reason, the following is needed for VS Code (through PyLance) to
# recognize that "error" is a valid module of the "taos" package.
from taos.error import *
from taos.field import *
from taos.result import *
from taos.schemaless import *
from taos.statement import *
from taos.statement2 import *
from taos.subscription import *

try:
    from taos.sqlalchemy import *
except:
    pass

from taos._version import __version__

# Globals
threadsafety = 2
"""sqlalchemy will read this attribute"""
paramstyle = "qmark"
"""sqlalchemy will read this attribute"""

__all__ = [
    "__version__",
    "IS_V3",
    "IGNORE",
    "TAOS_FIELD_COL",
    "TAOS_FIELD_TAG",
    "TAOS_FIELD_QUERY",
    "TAOS_FIELD_TBNAME",
    # functions
    "connect",
    "new_bind_param",
    "new_bind_params",
    "new_multi_binds",
    "new_multi_bind",
    # objects
    "TaosBind",
    "TaosConnection",
    "TaosCursor",
    "TaosResult",
    "TaosRows",
    "TaosRow",
    "TaosStmt",
    "TaosStmt2",
    "TaosStmt2Option",
    "BindTable",
    "PrecisionEnum",
    "SmlPrecision",
    "SmlProtocol",
    "utils",
]


def connect(*args, **kwargs):
    # type: (..., ...) -> TaosConnection
    """Function to return a TDengine connector object

    Current supporting keyword parameters:
    @dsn: Data source name as string
    @user: Username as string(optional)
    @password: Password as string(optional)
    @host: Hostname(optional)
    @database: Database name(optional)

    @rtype: TDengineConnector
    """
    return TaosConnection(*args, **kwargs)


IS_WS = os.getenv("TDENGINE_DRIVER", "native").lower() == "websocket"
