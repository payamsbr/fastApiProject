import configparser
import sqlite3


class GraphETLDataBase:
    def __init__(self):

        config = configparser.ConfigParser()
        config.read('config.ini')

        db_name = config['sqlite']['Name']
        self.con = sqlite3.connect(db_name, check_same_thread=False)

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        self.con.row_factory = dict_factory

        # initialize tables
        cur = self.con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS etl (
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
        update_interval INTEGER NOT NULL,
        enabled BOOELAN DEFAULT 1);''')

    def create_or_update(self, params, etl_id=None):
        cur = self.con.cursor()
        if etl_id is None:
            query = f'''INSERT INTO etl (from_column, to_column, from_node_type, to_node_type, edge_formula, 
            relation_type, table_name, datetime_column, des, start_date, end_date, update_interval, enabled) 
            VALUES ('{params.from_column}','{params.to_column}','{params.from_node_type}','{params.to_node_type}',
            '{params.edge_formula}','{params.relation_type}','{params.table_name}','{params.datetime_column}',
            '{params.des}','{params.start_date}','{params.end_date}',{params.update_interval},{params.enabled});'''
        else:
            query = f'''UPDATE etl SET from_column='{params.from_column}', to_column='{params.to_column}', 
            from_node_type='{params.from_node_type}', to_node_type='{params.to_node_type}', 
            edge_formula='{params.edge_formula}', relation_type='{params.relation_type}', 
            table_name='{params.table_name}', datetime_column='{params.datetime_column}', 
            des='{params.des}', start_date='{params.start_date}', end_date='{params.end_date}', 
            update_interval={params.update_interval}, enabled={params.enabled} WHERE id = {etl_id};'''
        cur.execute(query)
        print(query)
        self.con.commit()

    def list_with_page(self, page=1):
        cur = self.con.cursor()
        offset = (page - 1) * 10
        results = cur.execute(f'''SELECT * FROM etl LIMIT 10 OFFSET {offset};''').fetchall()
        return results
