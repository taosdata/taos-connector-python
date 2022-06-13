# TDengine Connector for Python

[TDengine](https://github.com/taosdata/TDengine) connector for Python enables python programs to access TDengine, using an API which is compliant with the Python DB API 2.0 (PEP-249). It contains two modules:

1. The `taos` module. It uses TDengine C client library for client server communications.
2. The `taosrest` module. It wraps TDengine RESTful API to Python DB API 2.0 (PEP-249). With this module, you are free to install TDengine C client library.

## Install

You can use `pip` to install the connector from PyPI:

```bash
pip install taospy
```

Or with git url:

```bash
pip install git+https://github.com/taosdata/taos-connector-python.git
```

## Source Code

[TDengine](https://github.com/taosdata/TDengine) connector for Python source code is hosted on [GitHub](https://github.com/taosdata/taos-connector-python).

## Examples for `taosrest` Module

### Query with PEP-249 API

```python
import taosrest

# all parameters are optional
conn = taosrest.connect(url="http://localhost:6041",
                        user="root",
                        password="taosdata")
cursor = conn.cursor()

cursor.execute("show databases")
results: list[tuple] = cursor.fetchall()
for row in results:
    print(row)
```

### Query with query method

```python
from taosrest import connect, TaosRestConnection, Result

conn: TaosRestConnection = connect()
result: Result = conn.query("show databases")

num_of_fields = result.field_count
for field in result.fields:
    print(field)

for row in result:
    print(row)
```

### Read with Pandas

#### Method one

```python
import pandas
import taosrest

conn = taosrest.connect()
df: pandas.DataFrame = pandas.read_sql("select * from log.logs", conn)
```

#### Method Two

```python
import pandas
from sqlalchemy import create_engine

engine = create_engine("taosrest://root:taosdata@localhost:6041")
df: pandas.DataFrame = pandas.read_sql("select * from log.logs", engine)
```

## Examples for `taos` Module

### Connect options

Supported config options:

- **config**: TDengine client configuration directory, by default use "/etc/taos/".
- **host**: TDengine server host, by default use "localhost".
- **user**: TDengine user name, default is "root".
- **password**: TDengine user password, default is "taosdata".
- **database**: Default connection database name, empty if not set.
- **timezone**: Timezone for timestamp type (which is `datetime` object with tzinfo in python) no matter what the host's timezone is.

```python
import taos

# 1. with empty options, connect TDengine by default options
#   that means:
#     - use /etc/taos/taos.cfg as default configuration file
#     - use "localhost" if not set in config file
#     - use "root" as default username
#     - use "taosdata" as default password
#     - use 6030 as default port if not set in config file
#     - use local timezone datetime as timestamp
conn = taos.connect()
# 2. with full set options, default db: log, use UTC datetime.
conn = taos.connect(host='localhost',
                    user='root',
                    password='taosdata',
                    database='log',
                    config='/etc/taos',
                    timezone='UTC')
```

Note that, the datetime formatted string will contain timezone information when timezone set. For example:

```python
# without timezone(local timezone depends on your machine)
'1969-12-31 16:00:00'
# with timezone UTC
'1969-12-31 16:00:00+00:00'
```

### Query with PEP-249 API

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("show databases")
results = cursor.fetchall()
for row in results:
    print(row)

cursor.close()
conn.close()
```

### Query with objective API

```python
import taos

conn = taos.connect()
conn.execute("create database if not exists pytest")

result = conn.query("show databases")
num_of_fields = result.field_count
for field in result.fields:
    print(field)

for row in result:
    print(row)

result.close()
conn.execute("drop database pytest")
conn.close()
```

### Query with async API

```python
from taos import *
from ctypes import *
import time


def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        result.close()
        return

    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None

    for row in result.rows_iter(num_of_rows):
        # print(row)
        None

    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)


def query_callback(p_param, p_result, code):
    # type: (c_void_p, c_void_p, c_int) -> None
    if p_result is None:
        return

    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)

    result.check_error(code)


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    # type: (TaosConnection) -> None
    counter = Counter(count=0)
    conn.query_a("select * from log.log", query_callback, byref(counter))

    while not counter.done:
        print("wait query callback")
        time.sleep(1)

    print(counter)
    conn.close()


if __name__ == "__main__":
    test_query(connect())
```

### Statement API - Bind row after row

```python
from taos import *

conn = connect()

dbname = "pytest_taos_stmt"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)

conn.execute(
    "create table if not exists log(ts timestamp, bo bool, nil tinyint, \
        ti tinyint, si smallint, ii int, bi bigint, tu tinyint unsigned, \
        su smallint unsigned, iu int unsigned, bu bigint unsigned, \
        ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
)

stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

params = new_bind_params(16)
params[0].timestamp(1626861392589)
params[1].bool(True)
params[2].tinyint(None)
params[3].tinyint(2)
params[4].smallint(3)
params[5].int(4)
params[6].bigint(5)
params[7].tinyint_unsigned(6)
params[8].smallint_unsigned(7)
params[9].int_unsigned(8)
params[10].bigint_unsigned(9)
params[11].float(10.1)
params[12].double(10.11)
params[13].binary("hello")
params[14].nchar("stmt")
params[15].timestamp(1626861392589)
stmt.bind_param(params)

params[0].timestamp(1626861392590)
params[15].timestamp(None)
stmt.bind_param(params)
stmt.execute()

assert stmt.affected_rows == 2

result = conn.query("select * from log")

for row in result:
    print(row)
```

### Statement API - Bind multi rows

```python
from taos import *

conn = connect()

dbname = "pytest_taos_stmt"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)

conn.execute(
    "create table if not exists log(ts timestamp, bo bool, nil tinyint, \
        ti tinyint, si smallint, ii int, bi bigint, tu tinyint unsigned, \
        su smallint unsigned, iu int unsigned, bu bigint unsigned, \
        ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
)

stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

params = new_multi_binds(16)
params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
params[1].bool((True, None, False))
params[2].tinyint([-128, -128, None])  # -128 is tinyint null
params[3].tinyint([0, 127, None])
params[4].smallint([3, None, 2])
params[5].int([3, 4, None])
params[6].bigint([3, 4, None])
params[7].tinyint_unsigned([3, 4, None])
params[8].smallint_unsigned([3, 4, None])
params[9].int_unsigned([3, 4, None])
params[10].bigint_unsigned([3, 4, None])
params[11].float([3, None, 1])
params[12].double([3, None, 1.2])
params[13].binary(["abc", "dddafadfadfadfadfa", None])
params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
params[15].timestamp([None, None, 1626861392591])
stmt.bind_param_batch(params)
stmt.execute()

assert stmt.affected_rows == 3

result = conn.query("select * from log")
for row in result:
    print(row)
```

### Subscription

```python
import taos
import random

conn = taos.connect()
dbname = "pytest_taos_subscribe"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s" % dbname)
conn.select_db(dbname)
conn.execute("create table if not exists log(ts timestamp, n int)")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % i)

sub = conn.subscribe(False, "test", "select * from log", 1000)
print("# consume from begin")
for ts, n in sub.consume():
    print(ts, n)

print("# consume new data")
for i in range(5):
    conn.execute("insert into log values(now, %d)(now+1s, %d)" % (i, i))
    result = sub.consume()
    for ts, n in result:
        print(ts, n)

sub.close(True)
print("# keep progress consume")
sub = conn.subscribe(False, "test", "select * from log", 1000)
result = sub.consume()
rows = result.fetch_all()
# consume from latest subscription needs root privilege(for /var/lib/taos).
assert result.row_count == 0
print("## consumed ", len(rows), "rows")

print("# consume with a stop condition")
for i in range(10):
    conn.execute("insert into log values(now, %d)" % random.randint(0, 10))
    result = sub.consume()
    try:
        ts, n = next(result)
        print(ts, n)
        if n > 5:
            result.stop_query()
            print("## stopped")
            break
    except StopIteration:
        continue

sub.close()
# sub.close()

conn.execute("drop database if exists %s" % dbname)
# conn.close()
```

### Subscription asynchronously with callback

```python
from taos import *
from ctypes import *

import time


def subscribe_callback(p_sub, p_result, p_param, errno):
    # type: (c_void_p, c_void_p, c_void_p, c_int) -> None
    print("# fetch in callback")
    result = TaosResult(c_void_p(p_result))
    result.check_error(errno)
    for row in result.rows_iter():
        ts, n = row()
        print(ts, n)


def test_subscribe_callback(conn):
    # type: (TaosConnection) -> None
    dbname = "pytest_taos_subscribe_callback"
    try:
        print("drop if exists")
        conn.execute("drop database if exists %s" % dbname)
        print("create database")
        conn.execute("create database if not exists %s" % dbname)
        print("create table")
        # conn.execute("use %s" % dbname)
        conn.execute("create table if not exists %s.log(ts timestamp, n int)" % dbname)

        print("# subscribe with callback")
        sub = conn.subscribe(False, "test", "select * from %s.log" % dbname, 1000, subscribe_callback)

        for i in range(10):
            conn.execute("insert into %s.log values(now, %d)" % (dbname, i))
            time.sleep(0.7)

        sub.close()

        conn.execute("drop database if exists %s" % dbname)
        # conn.close()
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        # conn.close()
        raise err


if __name__ == "__main__":
    test_subscribe_callback(connect())
```

### Insert with line protocol

```python
import taos
from taos import SmlProtocol, SmlPrecision

conn = taos.connect()
dbname = "pytest_line"
conn.execute("drop database if exists %s" % dbname)
conn.execute("create database if not exists %s precision 'us'" % dbname)
conn.select_db(dbname)

lines = [
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"pass",c2=false,c4=4f64 1626006833639000000',
]
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)
```

### Read with Pandas

#### Method one

```python
import pandas
import taos

conn = taos.connect()
df: pandas.DataFrame = pandas.read_sql("select * from log.logs", conn)
```

#### Method Two

```python
import pandas
from sqlalchemy import create_engine

engine = create_engine("taos://root:taosdata@localhost:6030/log")
df: pandas.DataFrame = pandas.read_sql("select * from logs", engine)
```

## Limitation

- `taosrest` is designed to use with taosAdapter. If your TDengine version is older than v2.4.0.0, taosAdapter may not be available.

## License

We use MIT license for Python connector.
