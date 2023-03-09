import configparser
import sqlite3
from queue import Queue


class GraphETLDataBase:
    dbName: str = None
    dbMaxApiCon: int = None
    dbConPool: Queue = None

    def __init__(self):

        config = configparser.ConfigParser()
        config.read('config.ini')
        self.dbName = config['sqlite']['Name']
        self.dbMaxApiCon = int(config['sqlite']['MaxApiConnections'])

        # setup shared connection pool (used for threads)
        max_pool_size = int(config['sqlite']['MaxPoolSize'])
        self.dbConPool = Queue(maxsize=max_pool_size)
        for i in range(max_pool_size):
            con = sqlite3.connect(self.dbName, check_same_thread=False)
            self.dbConPool.put(con)

        # initialize tables
        self._execute_single_query('''CREATE TABLE IF NOT EXISTS etl (
        id INTEGER PRIMARY KEY, 
        from_column TEXT NOT NULL,
        to_column TEXT NOT NULL,
        from_node_type TEXT NOT NULL,
        to_node_type TEXT NOT NULL,
        edge_formula TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        table_name TEXT NOT NULL,
        datetime_column TEXT NOT NULL,
        update_at DATETIME,
        des TEXT,
        log TEXT,
        log_date DATETIME,
        start_date DATETIME NOT NULL,
        end_date DATETIME NOT NULL,
        jump_type INTEGER,
        update_interval INTEGER NOT NULL,
        enabled BOOELAN DEFAULT TRUE);''', _commit=True)

    def _prepare_db_connection(self):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        con = sqlite3.connect(self.dbName, check_same_thread=True)
        con.row_factory = dict_factory
        self.dbMaxApiCon -= 1
        return con

    def _terminate_db_connection(self, con: sqlite3.Connection):
        con.close()
        self.dbMaxApiCon += 1

    def _execute_single_query(self, _query: str, _result: bool = False, _commit: bool = True) -> list | bool | None:
        con = self._prepare_db_connection()
        cur = con.cursor()
        cur.execute(_query)
        if _commit:
            con.commit()
        if _result:
            result = cur.fetchall()
            self._terminate_db_connection(con)
            return result
        self._terminate_db_connection(con)

    def create_or_update(self, params, etl_id=None):
        if etl_id is None:
            query = f'''INSERT INTO etl (from_column,to_column,from_node_type,to_node_type,edge_formula, 
            relation_type,table_name,datetime_column,des,start_date,end_date,jump_type,update_interval,enabled) 
            VALUES ('{params.from_column}','{params.to_column}','{params.from_node_type}','{params.to_node_type}',
            '{params.edge_formula}','{params.relation_type}','{params.table_name}','{params.datetime_column}',
            '{params.des}','{params.start_date}','{params.end_date}',{params.jump_type},{params.update_interval},
            {params.enabled});'''
        else:
            query = f'''UPDATE etl SET from_column='{params.from_column}',to_column='{params.to_column}', 
            from_node_type='{params.from_node_type}',to_node_type='{params.to_node_type}', 
            edge_formula='{params.edge_formula}',relation_type='{params.relation_type}', 
            table_name='{params.table_name}',datetime_column='{params.datetime_column}', 
            des='{params.des}',start_date='{params.start_date}',end_date='{params.end_date}',
            jump_type={params.jump_type},update_interval={params.update_interval},enabled={params.enabled} 
            WHERE id = {etl_id};'''
        self._execute_single_query(query, _commit=True)

    def list_with_page(self, page: int = 1) -> list:
        offset = (page - 1) * 10
        return self._execute_single_query(f'SELECT * FROM etl LIMIT 10 OFFSET {offset};', _result=True)
