import taosrest
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

conn = taosrest.connect(url=url, token=token)
c = conn.cursor()
c.execute("show databases")
data = c.fetchall()
print(data)