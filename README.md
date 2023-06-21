# TDengine Connector for Python

| Github Workflow | PyPI Version | PyPI Downloads | CodeCov |
| --------------- | ------------ | -------------- | ------- |
| ![workflow](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-python/test.yml) | ![PyPI](https://img.shields.io/pypi/v/taospy) | ![PyPI](https://img.shields.io/pypi/dm/taospy) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-python/branch/main/graph/badge.svg?token=BDANN3DBXS)](https://codecov.io/gh/taosdata/taos-connector-python) |




[TDengine](https://github.com/taosdata/TDengine) connector for Python enables python programs to access TDengine, using
an API which is compliant with the Python DB API 2.0 (PEP-249). It contains two modules:

1. The `taos` module. It uses TDengine C client library for client server communications.
2. The `taosrest` module. It wraps TDengine RESTful API to Python DB API 2.0 (PEP-249). With this module, you do not need to install the TDengine C client library.

## Install taospy

You can use `pip` to install the connector from PyPI:

```bash
pip install taospy
```

Or with git url:

```bash
pip install git+https://github.com/taosdata/taos-connector-python.git
```

Note: taospy v2.7.2 requirs Python 3.6+. The early versions of taospy from v2.5.0 to v2.7.1 require Python 3.7+.

## Source Code

[TDengine](https://github.com/taosdata/TDengine) connector for Python source code is hosted
on [GitHub](https://github.com/taosdata/taos-connector-python).

## Install taos-ws-py

### Install with taospy

```bash
pip install taospy[ws]
```

### Install taos-ws-py only

```bash
pip install taos-ws-py
```

Note: The taosws module is provided by taos-ws-py package separately from v2.7.2. It is part of early version of taospy.
taos-ws-py requires Python 3.7+.

### Query with PEP-249 API using `taosws`

```python
import taosws

# all parameters are optional
conn = taosws.connect("taosws://root:taosdata@localhost:6041")
cursor = conn.cursor()

cursor.execute("show databases")
results: list[tuple] = cursor.fetchall()
for row in results:
    print(row)
```

You can pass an optional req_id in the parameters.

```python
import taosws

# all parameters are optional
conn = taosws.connect("taosws://root:taosdata@localhost:6041")
cursor = conn.cursor()

cursor.execute("show databases", req_id=1)
results: list[tuple] = cursor.fetchall()
for row in results:
    print(row)
```

### Query with query method using `taosws`

```python
from taosws import *

conn = connect("taosws://root:taosdata@localhost:6041")
result = conn.query("show databases")

num_of_fields = result.field_count
for field in result.fields:
    print(field)

for row in result:
    print(row)
```

You can pass an optional req_id in the parameters.

```python
from taosws import *

conn = connect("taosws://root:taosdata@localhost:6041")
result = conn.query("show databases", req_id=1)

num_of_fields = result.field_count
for field in result.fields:
    print(field)

for row in result:
    print(row)
```

### Read with Pandas using `taosws`

#### Method one

```python
import pandas
import taosws

conn = taosws.connect("taosws://root:taosdata@localhost:6041")
df: pandas.DataFrame = pandas.read_sql("show databases", conn)
df
```

#### Method Two

```python
import pandas
from sqlalchemy import create_engine

engine = create_engine("taosws://root:taosdata@localhost:6041")
df: pandas.DataFrame = pandas.read_sql("show databases", engine)
df
```

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

You can pass an optional req_id in the parameters.

```python
import taosrest

# all parameters are optional
conn = taosrest.connect(url="http://localhost:6041",
                        user="root",
                        password="taosdata")
cursor = conn.cursor()

cursor.execute("show databases", req_id=1)
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

You can pass an optional req_id in the parameters.

```python
from taosrest import connect, TaosRestConnection, Result

conn: TaosRestConnection = connect()
result: Result = conn.query("show databases", req_id=1)

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
import taos

conn = taos.connect()
df: pandas.DataFrame = pandas.read_sql("select * from log.logs", conn)
```

#### Method two

```python
import pandas
import taosrest

conn = taosrest.connect()
df: pandas.DataFrame = pandas.read_sql("select * from log.logs", conn)
```

#### Method three

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
- **timezone**: Timezone for timestamp type (which is `datetime` object with tzinfo in python) no matter what the host's
  timezone is.

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

You can pass an optional req_id in the parameters.

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("show databases", req_id=1)
results = cursor.fetchall()
for row in results:
    print(row)

cursor.close()
conn.close()
```

#### Execute many

Method execute_many is supported:

There are two ways to use execute_many.

The first way is to pass a data_set as a list of dictionaries, where each dictionary will be expanded into a complete
statement.

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

db_name = "test_db"

cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
cursor.execute(f"CREATE DATABASE {db_name}")
cursor.execute(f"USE {db_name}")

cursor.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

create_table_data = [
    {
        "name": "tb1",
        "t1": 1,
    },
    {
        "name": "tb2",
        "t1": 2,
    },
    {
        "name": "tb3",
        "t1": 3,
    }
]

cursor.execute_many(
    "create table {name} using stb tags({t1})",
    create_table_data,
)

```

The second way is to pass a data_set as a list of tuples, where each tuple represents a different row of the same
table, and each value within the tuple will become a data row in the SQL statement. All the data together will form a
complete SQL statement.

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

db_name = "test_db"

cursor.execute(f"USE {db_name}")

data_set = [
    ('2018-10-03 14:38:05.100', 219),
    ('2018-10-03 14:38:15.300', 218),
    ('2018-10-03 14:38:16.800', 221),
]

table_name = "tb1"

cursor.execute_many(f"insert into {table_name} values", data_set)

```

[Example: Insert many lines in one execute](./examples/cursor_execute_many.py)

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

You can pass an optional req_id in the parameters.

```python
import taos

conn = taos.connect()
conn.execute("create database if not exists pytest", req_id=1)

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
        pass

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

You can pass an optional req_id in the parameters.

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
        pass

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
    conn.query_a("select * from log.log", query_callback, byref(counter), req_id=1)

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

You can pass an optional req_id in the parameters.

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
conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=1)
print("inserted")

conn.schemaless_insert(lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.NOT_CONFIGURED, req_id=2)

result = conn.query("show tables")
for row in result:
    print(row)

conn.execute("drop database if exists %s" % dbname)
```

Insert with schemaless raw data.

```python
import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        res = conn.schemaless_insert_raw(lines, 1, 0)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except SchemalessError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

```

Pass optional ttl in the parameters.

```python
import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    ttl = 1000
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    ttl = 1000
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        ttl = 1000
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err
```

Pass optional req_id in the parameters.

```python
import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    ttl = 1000
    req_id = utils.gen_req_id()
    res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
        # assert (err.msg == 'Invalid data format')

    result = conn.query("select * from st")
    print(result.row_count)
    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()

    conn.execute("drop database if exists %s" % dbname)
    conn.close()
except InterfaceError as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
except Exception as err:
    conn.execute("drop database if exists %s" % dbname)
    conn.close()
    print(err)
    raise err

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

- `taosrest` is designed to use with taosAdapter. If your TDengine version is older than v2.4.0.0, taosAdapter may not
  be available.

## License

We use MIT license for Python connector.
