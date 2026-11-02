"""验证 I/O 期间让出 GIL，真实数据库测试只允许连接本机临时数据库。"""
import contextlib
import os
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest
import taosws


def sample_threads(call):
    """测量被调用函数运行期间，另一个 Python 线程的最大暂停时间。"""
    ticks = []
    stop = threading.Event()

    def heartbeat():
        """每毫秒尝试获得 GIL，记录实际执行机会。"""
        while not stop.is_set():
            ticks.append(time.perf_counter())
            time.sleep(0.001)

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    time.sleep(0.03)
    start = time.perf_counter()
    try:
        result = call()
    finally:
        end = time.perf_counter()
        stop.set()
        thread.join(timeout=2)
    inside = [t for t in ticks if start < t < end]
    points = [start, *inside, end]
    gap = max(b - a for a, b in zip(points, points[1:]))
    return result, end - start, gap


@contextlib.contextmanager
def network_process(mode):
    """使用独立进程制造网络等待，避免测试服务器受被测进程 GIL 影响。"""
    process = subprocess.Popen([sys.executable, __file__, mode], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    port = int(process.stdout.readline())
    try:
        yield port
    finally:
        process.terminate()
        process.communicate(timeout=5)


def test_connect_releases_gil():
    """延迟失败的 WebSocket 握手也必须让其他 Python 线程继续执行。"""
    with network_process('reject') as port:
        def connect():
            """尝试连接可控的失败服务端。"""
            with pytest.raises(taosws.Error):
                taosws.connect(f'taosws://127.0.0.1:{port}')

        _, elapsed, gap = sample_threads(connect)
    assert elapsed >= 0.45
    assert gap < 0.30, (elapsed, gap)


@pytest.fixture(scope='module')
def local_database():
    """仅在显式开启集成测试时创建本机一次性数据库。"""
    if os.environ.get('TAOSWS_LOCAL_INTEGRATION') != '1':
        pytest.skip('requires disposable local TDengine container')
    deadline = time.monotonic() + 90
    while True:
        try:
            connection = taosws.connect('taosws://root:taosdata@127.0.0.1:6041')
            break
        except taosws.Error:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    database = 'gil_regression_' + uuid.uuid4().hex
    connection.execute(f'CREATE DATABASE {database}')
    try:
        connection.execute(f'CREATE TABLE {database}.bars (ts TIMESTAMP, reading BIGINT, price DOUBLE)')
        for start in range(0, 12000, 1000):
            values = ''.join(f'({1700000000000 + i},{i},{i + 0.25})' for i in range(start, start + 1000))
            connection.execute(f'INSERT INTO {database}.bars VALUES {values}')
        yield database
    finally:
        connection.execute(f'DROP DATABASE {database}')
        connection.close()


def test_cursor_fetch_and_query_releases_gil(local_database):
    """通过延迟代理验证查询、跨块取数、迭代接口及 Python 方法签名。"""
    with network_process('proxy') as port:
        connection = taosws.connect(f'taosws://root:taosdata@127.0.0.1:{port}')
        try:
            cursor, _, _ = sample_threads(connection.cursor)
            sql = f'SELECT reading, price FROM {local_database}.bars ORDER BY ts'
            _, elapsed, gap = sample_threads(lambda: cursor.execute(sql))
            assert elapsed < 0.04 or gap < elapsed * 0.75, (elapsed, gap)
            rows, elapsed, gap = sample_threads(cursor.fetchall)
            assert len(rows) == 12000
            assert rows[0] == (0, 0.25)
            assert rows[-1] == (11999, 11999.25)
            assert elapsed < 0.04 or gap < elapsed * 0.75, (elapsed, gap)
            cursor.execute(sql)
            assert cursor.fetchone() == rows[0]
            assert cursor.fetchmany(17) == rows[1:18]
            remaining = []
            while batch := cursor.fetchmany(1000):
                remaining.extend(batch)
            assert remaining == rows[18:]
            cursor.execute(sql)
            assert cursor.fetchallintodict()[0] == {'reading': 0, 'price': 0.25}
            cursor.execute(sql)
            assert cursor.fetch_all_into_dict()[-1] == {'reading': 11999, 'price': 11999.25}
            assert list(connection.query(sql)) == rows
            assert list(connection.query_with_req_id(sql, 54321)) == rows
            cursor.close()
        finally:
            connection.close()


def relay(source, destination, delay):
    """转发 TCP 数据，并在服务端响应方向加入固定延迟。"""
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            if delay:
                time.sleep(delay)
            destination.sendall(data)
    except OSError:
        pass
    finally:
        with contextlib.suppress(OSError):
            destination.shutdown(socket.SHUT_RDWR)
        source.close()


def run_network_server(mode):
    """运行独立测试服务端，不接受外部主机或生产数据库配置。"""
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen()
        print(listener.getsockname()[1], flush=True)
        while True:
            client, _ = listener.accept()
            if mode == 'reject':
                client.recv(4096)
                time.sleep(0.5)
                with contextlib.suppress(OSError):
                    client.sendall(b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n')
                client.close()
                continue
            upstream = socket.create_connection(('127.0.0.1', 6041), timeout=10)
            upstream.settimeout(None)
            threading.Thread(target=relay, args=(client, upstream, 0), daemon=True).start()
            threading.Thread(target=relay, args=(upstream, client, 0.075), daemon=True).start()


if __name__ == '__main__':
    run_network_server(sys.argv[1])
