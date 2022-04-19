class TaosRestConnection:
    def __init__(self, **kwargs):
        self._host = kwargs["host"] if "host" in kwargs else "localhost"
        self._port = kwargs["port"] if "port" in kwargs else 6041
        self._user = kwargs["user"] if "user" in kwargs else "root"
        self._password = kwargs["password"] if "password" in kwargs else "taosdata"

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        pass
