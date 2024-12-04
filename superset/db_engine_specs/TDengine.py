# TDengine driver for Apache SuperSet
from superset.db_engine_specs.base import  BaseEngineSpec

class TDengineEngineSpec(BaseEngineSpec):
    engine_name = "TDengine"  # show name in SuperSet driver list
    engine      = "taosws"
    max_column_name_length = 64
    default_driver = "taosws"
    sqlalchemy_uri_placeholder = (
        "protocol://user:password@host:port/dbname[?key=value&key=value...]"    )