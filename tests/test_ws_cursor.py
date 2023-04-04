import datetime
import taosws

env = {
    'db_protocol': "taosws",
    'user': "root",
    'password': "taosdata",
    'host': "localhost",
    'port': 6041,
}


def make_context(config):
    db_protocol = config.get('db_protocol', 'taos')
    db_user = config['user']
    db_pass = config['password']
    db_host = config['host']
    db_port = config['port']

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"
    print('dsn: ', db_url)

    conn = taosws.connect(db_url)

    db_name = config.get('database', 'c_cursor')

    return conn, db_name


def test_cursor_execute_many():
    conn, db = make_context(env)
    cur = conn.cursor()
    res = cur.execute('show dnodes', 1)
    print(f'res: {res}')
    # get column names from cursor
    column_names = [meta[0] for meta in cur.description]
    print(column_names)
    data = cur.fetchall()
    for r in data:
        print(r)

    cur.execute(f"drop database if exists {db}")
    cur.execute(f"create database {db}")
    cur.execute("use {name}", name=db)
    cur.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    data = [
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

    res = cur.execute_many(
        "create table {name} using stb tags({t1})",
        data,
    )
    print(f'res: {res}')

    ts = datetime.datetime.now().astimezone()
    data = [
        ("tb1", ts, 1),
        ("tb2", ts, 2),
        ("tb3", ts, 3),
    ]
    cur.execute_many(
        "insert into {} values('{}', {})",
        data,
    )
    cur.execute('select * from stb')
    data = cur.fetchall()
    column_names = [meta[0] for meta in cur.description]
    print(column_names)
    for r in data:
        print(r)
    cur.close()
    conn.close()
