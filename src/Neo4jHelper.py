"""
read this "https://neo4j.com/docs/python-manual/current/transactions/"
==========================================================================
Session creation is a lightweight operation,
so sessions can be created and destroyed without significant cost.
Always close sessions when you are done with them.
==========================================================================
Sessions are not thread safe: share the main Driver object across threads,
but make sure each thread creates its own sessions.
"""
from queue import Queue
import configparser
from neo4j import GraphDatabase, Session, ManagedTransaction, exceptions


class Neo4jHelper:
    sessionPool: "Queue[Session]" = None

    def __init__(self):

        # read configurations
        config = configparser.ConfigParser()
        config.read('config.ini')
        host = config['neo4j']['Ip']
        port = config['neo4j']['Port']
        user = config['neo4j']['User']
        password = config['neo4j']['Pass']
        pool_size = int(config['neo4j']['PoolSize'])
        uri = f"bolt://{host}:{port}"

        # prepare database
        driver = GraphDatabase.driver(uri, auth=(user, password))

        # prepare pool
        self.sessionPool = Queue(maxsize=pool_size)
        for i in range(0, pool_size):
            session = driver.session()
            self.sessionPool.put(session)

    # requested from threads
    def submit_data(self, data):
        session = self.sessionPool.get()
        try:
            session.execute_write(self._transaction_func, data)
        except exceptions.Neo4jError as e:
            print(e)
        self.sessionPool.put(session)

    @staticmethod
    def _transaction_func(tx: ManagedTransaction, data_list: list):
        for data in data_list:
            d = {"num1": data[0], "num2": data[1], "sum": data[2], "count": data[3], "min": data[4], "max": data[5]}
            query = f"""MERGE (n1:TNumber {{name: '{d["num1"]}'}})
MERGE (n2:TNumber {{name:'{d["num2"]}'}})
MERGE (n1)-[r:TCall]-(n2)
ON CREATE SET r.sum={d["sum"]}, r.count={d["count"]}, r.min={d["min"]}, r.max={d["max"]}
ON MATCH SET r.sum=r.sum+{d["sum"]}, r.count=r.count+{d["count"]}, 
    r.min=(CASE WHEN r.min<{d["min"]} THEN r.min ELSE {d["min"]} END), 
    r.max=(CASE WHEN r.max>{d["max"]} THEN r.max ELSE {d["max"]} END)"""
            # print(query)
            tx.run(query)
        # result = tx.run("MATCH (c:Person) return c")
        # print('done')
        # print(result)
        # records = list(result)
        # summary = result.consume()
        # print(records)
        # print(summary)
        # return records
