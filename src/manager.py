"""
manager is an interval running in separate thread (not main thread)
to frequently check and moderate (ETL)'s thread pool, prepare and control
thread workers
"""

import configparser
from threading import Timer
from models.ModelEtl import EnumJumpTypes
from src.db import GraphETLDataBase
from src.ClickHouseHelper import ClickHouseHelper
from src.Neo4jHelper import Neo4jHelper
from concurrent.futures import ThreadPoolExecutor
import datetime


class EtlManager(object):
    def __init__(self, database: GraphETLDataBase):

        config = configparser.ConfigParser()
        config.read('config.ini')

        # setup timer
        self._timer = None
        self.interval = float(config['etl']['managerInterval'])
        self.database = database
        self.is_running = False

        # setup clickhouse
        self.clickHousePool = ClickHouseHelper().clientPool

        # setup neo4j
        self.neo4jHelper = Neo4jHelper()

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
        # scan database, list ETL records
        con = self.database.dbConPool.get()
        cur = con.cursor()
        cur.execute('SELECT * FROM [etl] WHERE [enabled]=1 AND [busy]=0')
        etl_records = cur.fetchall()
        # create workers per each ETL
        for etl in etl_records:
            # update ETL record (mark as busy & update cursor)
            if etl['jump_type'] == EnumJumpTypes.byDate.value:
                start_timestamp = datetime.datetime.fromisoformat(etl['jump_start']).timestamp()
                end_timestamp = datetime.datetime.fromisoformat(etl['jump_end']).timestamp()
                cursor_timestamp = etl['cursor']
                # initialize cursor
                if cursor_timestamp is None or len(cursor_timestamp) == 0:
                    cursor_timestamp = start_timestamp
                # disable ETL if reaches the end
                if cursor_timestamp >= end_timestamp:
                    cur.execute(f"UPDATE [etl] SET [enabled]=0 WHERE [id]={etl['id']}")
                    con.commit()
                    continue
                old_cursor = cursor_timestamp
                cursor_timestamp += (etl['jump_size'] * (60 * 60 * 24))
                # update sqlite for this ETL record
                cur.execute(f"UPDATE [etl] SET [busy]=1 WHERE [id]={etl['id']}")
                con.commit()
                # put new task to the pool
                start_datetime = f"'{datetime.datetime.fromtimestamp(old_cursor)}'"
                end_datetime = f"'{datetime.datetime.fromtimestamp(cursor_timestamp)}'"
                etl['cursor'] = cursor_timestamp
                self.etl_worker_pool.submit(self._worker_task, etl, start_datetime, end_datetime)
        # return connection to the pool
        self.database.dbConPool.put(con)

    def _worker_task(self, _etl, _start, _end):
        # fetch data from clickhouse
        clickhouse_client = self.clickHousePool.get()
        query = f"""SELECT {_etl['from_column']}, {_etl['to_column']}, SUM({_etl['avg_column']}) AS __SUM, 
        COUNT(*) AS __COUNT, MIN({_etl['avg_column']}) AS __MIN, MAX({_etl['avg_column']}) AS __MAX 
        FROM {_etl['table_name']} WHERE {_etl['jump_column']} BETWEEN {_start} AND {_end} 
        GROUP BY {_etl['from_column']}, {_etl['to_column']}"""
        results = clickhouse_client.execute(query)
        print(results)
        self.clickHousePool.put(clickhouse_client)
        # insert data to neo4j
        # update ETL record (sqlite) change cursor and remove busy
        con = self.database.dbConPool.get()
        cur = con.cursor()
        # todo, check ETL enabled and END limit, to set proper busy value
        end_timestamp = datetime.datetime.fromisoformat(_etl['jump_end']).timestamp()
        # terminate
        if _etl['cursor'] >= end_timestamp:
            cur.execute(f"UPDATE [etl] SET [cursor]={_etl['cursor']}, [busy]=0, [enabled]=0 WHERE [id]={_etl['id']}")
            con.commit()
        # keep running
        else:
            cur.execute(f"UPDATE [etl] SET [cursor]={_etl['cursor']} WHERE [id]={_etl['id']}")
            con.commit()
            print('done')
            # shift cursor
            old_cursor = _etl['cursor']
            if _etl['jump_type'] == EnumJumpTypes.byDate.value:
                _etl['cursor'] += _etl['jump_size'] * (60 * 60 * 24)
            else:
                _etl['cursor'] += _etl['jump_size']
            start_datetime = f"'{datetime.datetime.fromtimestamp(old_cursor)}'"
            end_datetime = f"'{datetime.datetime.fromtimestamp(_etl['cursor'])}'"
            self.etl_worker_pool.submit(self._worker_task, _etl, start_datetime, end_datetime)
        self.database.dbConPool.put(con)
        return
