"""
manager is an interval running in separate thread (not main thread)
to frequently check and moderate (ETL)'s thread pool, prepare and control
thread workers
"""

import configparser
from threading import Timer
from src.db import GraphETLDataBase
from concurrent.futures import ThreadPoolExecutor


class EtlManager(object):
    def __init__(self, database: GraphETLDataBase):

        config = configparser.ConfigParser()
        config.read('config.ini')

        # setup timer
        self._timer = None
        self.interval = float(config['etl']['managerInterval'])
        self.database = database
        self.is_running = False

        # setup thread pool (read configs)
        thread_num = int(config['etl']['Threads'])
        self.etl_worker_pool = ThreadPoolExecutor(max_workers=thread_num)

        # start timer
        self.start()

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

    def _run(self):
        self.is_running = False
        self._etl_check()
        self.start()

    def _etl_check(self):
        # scan database for ETL
        con = self.database.dbConPool.get()
        cur = con.cursor()
        cur.execute('SELECT * FROM [etl] WHERE [enabled] = 1')
        result = cur.fetchall()
        self.database.dbConPool.put(con)
        print(result)
