# encoding:UTF-8
from taos.connection import TaosConnection

# For some reason, the following is needed for VS Code (through PyLance) to
# recognize that "error" is a valid module of the "taos" package.
from taos.error import *
from taos.bind import *
from taos.field import *
from taos.cursor import *
from taos.result import *
from taos.statement import *
from taos.subscription import *
from taos.schemaless import *

try:
    from taos.sqlalchemy import *
except:
    pass

from taos._version import __version__

# Globals
threadsafety = 0
"""sqlalchemy will read this attribute"""
paramstyle = "pyformat"
"""sqlalchemy will read this attribute"""

__all__ = [
    "__version__",
    "IS_V3",
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
