#!

# Copyright (c) 2023 TAOS Data, Inc.
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

import taos
from taos.tmq import Consumer
import taosws


def prepare():
    conn = taos.connect()
    conn.execute("drop topic if exists tmq_assignment_demo_topic")
    conn.execute("drop database if exists tmq_assignment_demo_db")
    conn.execute("create database if not exists tmq_assignment_demo_db wal_retention_period 3600")
    conn.select_db("tmq_assignment_demo_db")
    conn.execute(
        "create table if not exists tmq_assignment_demo_table (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)"
    )
    conn.execute(
        "create topic if not exists tmq_assignment_demo_topic as select ts, c1, c2, c3 from tmq_assignment_demo_table"
    )
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-2s, 1, 1.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now-1s, 2, 2.0, 'tmq test')")
    conn.execute("insert into d0 using tmq_assignment_demo_table tags (0) values (now, 3, 3.0, 'tmq test')")


def taos_get_assignment_and_seek_demo():
    prepare()
    consumer = Consumer(
        {
            "group.id": "0",
            # should disable snapshot,
            # otherwise it will cause invalid params error
            "experimental.snapshot.enable": "false",
        }
    )
    consumer.subscribe(["tmq_assignment_demo_topic"])

    # get topic assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)

    # poll
    consumer.poll(1)
    consumer.poll(1)

    # get topic assignment again
    after_pool_assignments = consumer.assignment()
    for assignment in after_pool_assignments:
        print(assignment)

    # seek to the beginning
    for assignment in assignments:
        consumer.seek(assignment)

    # now the assignment should be the same as before poll
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)


def taosws_get_assignment_and_seek_demo():
    prepare()
    consumer = taosws.Consumer(
        conf={
            "td.connect.websocket.scheme": "ws",
            # should disable snapshot,
            # otherwise it will cause invalid params error
            "experimental.snapshot.enable": "false",
            "group.id": "0",
        }
    )
    consumer.subscribe(["tmq_assignment_demo_topic"])

    # get topic assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment.to_string())

    # poll
    consumer.poll(1)
    consumer.poll(1)

    # get topic assignment again
    after_poll_assignments = consumer.assignment()
    for assignment in after_poll_assignments:
        print(assignment.to_string())

    # seek to the beginning
    for assignment in assignments:
        for a in assignment.assignments():
            consumer.seek(assignment.topic(), a.vg_id(), a.offset())

    # now the assignment should be the same as before poll
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment.to_string())
