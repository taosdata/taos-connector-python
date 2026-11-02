# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import json
import requests
import taos

HISTORICAL_API = "https://production.api.coindesk.com/v2/tb/price/values/"

id = "BTC"
end_date = datetime.datetime.utcnow()
end_date_str = end_date.strftime("%FT%H:%M")

start_date = end_date - datetime.timedelta(hours=24)
start_date_str = start_date.strftime("%FT%H:%M")
url = "https://production.api.coindesk.com/v2/tb/price/values/%s?start_date=%s&end_date=%s&ohlc=false" % (
    id,
    start_date_str,
    end_date_str,
)

response = requests.get(url).json()
# response = json.load(open("examples/prices.json"))

entries = response["data"]["entries"]

taos = taos.connect(database="bpi")

for entry in entries:
    taos.execute("insert into usd values(%d, %d)" % (entry[0], entry[1]))
