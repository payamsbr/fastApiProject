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
from neo4j import GraphDatabase, Session


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
        return
