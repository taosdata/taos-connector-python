import sys
from pathlib import Path


_current_package_dir = Path(__file__).resolve().parent
for search_path in list(sys.path):
    candidate = Path(search_path) / "taosws"
    if candidate.is_dir() and candidate != _current_package_dir and str(candidate) not in __path__:
        __path__.append(str(candidate))

try:
    from . import _taosws as _native
    from ._taosws import *
except ImportError:
    from . import taosws as _native
    from .taosws import *

__doc__ = _native.__doc__
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
