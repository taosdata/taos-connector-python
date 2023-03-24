import taosws


def test_ws_connect():
    print('-' * 40)
    print('test_ws_connect')
    conn = taosws.connect('taosws://root:taosdata@localhost:6041')
    r = conn.query_with_req_id('show dnodes', 1)
    print('r: ', r.fields)
    print('test_ws_connect done')
    print('-' * 40)


def test_default_connect():
    print('-' * 40)
    print('test_default_connect')
    conn = taosws.connect()
    r = conn.query_with_req_id('show dnodes', 1)
    print('r: ', r.fields)
    print('test_default_connect done')
    print('-' * 40)


def test_native_connect():
    print('-' * 40)
    print('test_native_connect')
    try:
        conn = taosws.connect('taos://root:taosdata@localhost:6030')
        r = conn.query_with_req_id('show dnodes', 1)
        print('r: ', r.fields)

    except Exception as e:
        print('Exception: ', e)
    print('test_native_connect done')
    print('-' * 40)


def test_connect_with_args():
    print('-' * 40)
    print('test_mutil_connect')
    conn = taosws.connect(
        user='taos',
        password='taosdata',
        host='localhost',
        port=6030,
    )
    r = conn.query_with_req_id('show dnodes', 1)
    print('r: ', r.fields)
    print('test_mutil_connect done')
    print('-' * 40)


def show_env():
    import os
    print('-' * 40)
    print('TAOS_LIBRARY_PATH: ', os.environ.get('TAOS_LIBRARY_PATH'))
    print('TAOS_CONFIG_DIR: ', os.environ.get('TAOS_CONFIG_DIR'))
    print('TAOS_C_CLIENT_VERSION: ', os.environ.get('TAOS_C_CLIENT_VERSION'))
    print('TAOS_C_CLIENT_VERSION_STR: ', os.environ.get('TAOS_C_CLIENT_VERSION_STR'))
    print('taosws.__version__', taosws.__version__)
    print('-' * 40)


if __name__ == '__main__':
    show_env()
    test_ws_connect()
    test_default_connect()
    test_connect_with_args()
