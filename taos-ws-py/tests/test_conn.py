import taosws


def test_native_connect():
    print('-' * 40)
    print('test_native_connect')
    conn = taosws.connect()
    conn.query_with_req_id('show dnodes', 1)
    print('test_native_connect done')
    print('-' * 40)


def test_ws_connect():
    print('-' * 40)
    print('test_ws_connect')
    conn = taosws.connect('taosws://root:taosdata@localhost:6041')
    conn.query_with_req_id('show dnodes', 1)
    print('test_ws_connect done')
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
    test_ws_connect()
    test_native_connect()
