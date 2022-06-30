from functools import wraps
import os


def check_env(func):
    @wraps(func)
    def _func():
        if "TDENGINE_URL" not in os.environ:
            return
        func()

    return _func
