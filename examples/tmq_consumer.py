from taos.tmq import Consumer

if __name__ == '__main__':
    consumer = Consumer(
        {
            "group.id": "3",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "msg.with.table.name": "true",
            "enable.auto.commit": "true",
        }
    )
    consumer.subscribe(["topic1", "topic2"])

    while True:
        res = consumer.poll(100)
        if not res:
            continue
        err = res.error()
        if err is not None:
            raise err
        val = res.value()
        print(val)

    consumer.unsubscribe()
