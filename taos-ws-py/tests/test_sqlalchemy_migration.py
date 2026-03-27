import importlib
import ast
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy.engine.url import make_url
from sqlalchemy import types as sqltypes


def test_taosws_sqlalchemy_module_is_available():
    module = importlib.import_module("taosws.sqlalchemy")
    assert hasattr(module, "TaosWsDialect")


def test_resolve_type_covers_all_declared_tdengine_types():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()

    for tdengine_type, sqlalchemy_type in module.TYPES_MAP.items():
        assert dialect._resolve_type(tdengine_type) is sqlalchemy_type

    assert dialect._resolve_type("TYPE_NOT_EXISTS") is sqltypes.UserDefinedType


def test_create_connect_args_prefers_hosts_and_keeps_other_query_params():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url(
        "taosws://root:taosdata@localhost:6041/test_1755496227?"
        "hosts=localhost:6041,127.0.0.1:6041&timezone=Asia/Shanghai"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041,127.0.0.1:6041/test_1755496227?timezone=Asia/Shanghai"]
    assert kwargs == {}


def test_create_connect_args_no_trailing_ampersand_when_hosts_is_last_param():
    module = importlib.import_module("taosws.sqlalchemy")
    dialect = module.TaosWsDialect()
    url = make_url(
        "taosws://root:taosdata@localhost:6041/test_1755496227?" "timezone=Asia/Shanghai&hosts=localhost:6041"
    )

    args, kwargs = dialect.create_connect_args(url)

    assert args == ["taosws://root:taosdata@localhost:6041/test_1755496227?timezone=Asia/Shanghai"]
    assert kwargs == {}


def test_sqlalchemy_module_does_not_depend_on_taospy_imports():
    module = importlib.import_module("taosws.sqlalchemy")
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                top_level_name = alias.name.split(".")[0]
                assert top_level_name not in {"taos", "taospy"}
