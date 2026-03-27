import importlib
import importlib.util
import pkgutil


__path__ = pkgutil.extend_path(__path__, __name__)


def _load_native_module():
    for module_name in ("_taosws", "taosws"):
        full_name = f"{__name__}.{module_name}"
        if importlib.util.find_spec(full_name) is None:
            continue
        return importlib.import_module(full_name)

    raise ImportError(
        "Failed to import native extension 'taosws._taosws' or 'taosws.taosws'. "
        "Ensure taos-ws-py is built and installed correctly."
    )


_native = _load_native_module()

if hasattr(_native, "__all__"):
    for name in _native.__all__:
        globals()[name] = getattr(_native, name)
else:
    for name in dir(_native):
        if not name.startswith("_"):
            globals()[name] = getattr(_native, name)

__doc__ = _native.__doc__
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
