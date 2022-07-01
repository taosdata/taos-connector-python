from functools import wraps
import os


def check_env(func):
    @wraps(func)
    def _func():
        if "TDENGINE_URL" in os.environ and "SQLALCHEMY_URL" in os.environ:
            func()
        else:
            print("skip ", func.__name__)

    return _func
