import taosrest
import os


def connect_to_cloud_use_token(host, port, token):
    conn = taosrest.connect(host=host,
                            port=port,
                            username="root",
                            password="taosdata",
                            token=token)

    print(conn.server_info)
    cursor = conn.cursor()
    cursor.execute("drop database if exists pytest")
    cursor.execute("create database pytest precision 'ns' keep 365")
    cursor.execute("create table pytest.temperature(ts timestamp, temp int)")
    cursor.execute("insert into pytest.temperature values(now, 1) (now+10b, 2)")
    cursor.execute("select * from pytest.temperature")
    rows = cursor.fetchall()
    print(rows)


if __name__ == '__main__':
    test_host = os.environ["CLOUD_HOST"]
    test_port = os.environ["CLOUD_PORT"]
    test_token = os.environ["CLOUD_TOKEN"]
    connect_to_cloud_use_token(test_host, int(test_port), test_token)
