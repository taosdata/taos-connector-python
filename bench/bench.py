import argparse
import time
from tracemalloc import start
import taos
import random as rnd
import sys
import threading
import logging
import numpy as np

class InsertThreading(threading.Thread):
    def __init__(self, id, tables, benchmark):
        super(InsertThreading, self).__init__()
        self.id = id
        self.tables = tables
        self.benchmark = benchmark

    def run(self):
        logging.info(f"Thread {self.id} start")
        conn = taos.connect()
        conn.select_db("test")
        for table_id in self.tables:
            inserted_rows = 0
            while inserted_rows < self.benchmark.args.rows:
                sql = benchmark.get_sql(table_id, self.benchmark.start_time)
                affected_row = conn.execute(sql)
                if affected_row <= 0:
                    logging.error("failed to execute sql: {sql}")
                    exit(1)
                inserted_rows += affected_row
                

class Benchmark(object):
    """TDengine python connector benchmark"""

    def __init__(self):
        self.data = []
        self.start_time = 15000000000
        self.parse_arguments()
        logging.basicConfig(level=logging.DEBUG, 
                            datefmt='%Y-%m-%d %A %H:%M:%S',
                            format   = '%(asctime)s [%(levelname)s]: %(message)s')
        logging.info("thread: %d", self.args.threads)
        logging.info("table: %d", self.args.tables)
        logging.info("rows: %d", self.args.rows)
        logging.info("batch: %d", self.args.batch)
        self.prepare_random(self.args.random)

    def get_random(self, type, length=8):
        if type == "bool":
            if rnd.randint(0 , 1):
                return "true"
            else:
                return "false"
        elif type == "int":
            return str(rnd.randint(-2**31 + 1, 2**31 -1))
        elif type == "bigint" or type == "timestamp":
            return str(rnd.randint(-2**63 + 1, 2**63 -1))
        elif type == "float" or type == "double":
            return '%f'%rnd.uniform(1.0, 100.0)
        elif type == "binary" or type == "varchar" or type == "nchar":
            return ''.join(rnd.sample('abcdefghijklmnopqrstuvwxyz0123456789', length))
        elif type == "smallint":
            return str(rnd.randint(-32767, 32767))
        elif type == "tinyint":
            return str(rnd.randint(-127, 127))
        else:
            logging.error(f"Unknown type: {type}")
            exit(1)


    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='TDengine python connector benchmark')
        
        parser.add_argument('-t', default=10000, type=int, dest='tables', help='number of tables')
        parser.add_argument('-n', default=10000, type=int, dest='rows', help='number of rows')
        parser.add_argument('-T', default=8, type=int, dest='threads', help='number of threads')
        parser.add_argument('-F', default=10000, type=int, dest='random', help='prepared random data number')
        parser.add_argument('-r', default=10000, type=int, dest='batch', help='batch size')

        self.args = parser.parse_args()

    def prepare_random(self, random):
        for _ in range(random):
            raw_data = []
            raw_data.append(self.get_random("int"))
            raw_data.append(self.get_random("float"))
            raw_data.append(self.get_random("bool"))
            raw_data.append(self.get_random("binary"))
            single_col_data = ','.join(raw_data)
            self.data.append(single_col_data)

    def get_sql(self, table_num, start_time, data_index):
        sql = "insert into d_%s values" % table_num 
        for _ in range(self.args.batch):
            sql += "(" + str(start_time) + ","
            start_time += 1
            sql += self.data[data_index]
            data_index += 1
            if data_index >= len(self.data):
                data_index = 0
            sql += ") "



if __name__ == "__main__":
    benchmark = Benchmark()
    table_ids = np.arange(0, benchmark.args.tables, 1)
    table_ids_split = np.split(table_ids, benchmark.args.threads)
    threads = []
    start_time = time.time()
    for i in range(benchmark.args.threads):
        iThread = InsertThreading(i, table_ids_split[i], benchmark)
        iThread.start()
        threads.append(iThread)
    
    for t in threads:
        t.join()

    end_time = time.time()

    logging.info(f"spend {end_time - start_time} seconds insert into {benchmark.args.tables} tables with {benchmark.args.rows} rows for each")