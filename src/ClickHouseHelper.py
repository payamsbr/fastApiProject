from queue import Queue
import configparser
from clickhouse_driver import Client


class ClickHouseHelper:
    clientPool: Queue[Client] = None

    def __init__(self):
        # read configurations
        config = configparser.ConfigParser()
        config.read('config.ini')
        host = config['clickhouse']['Ip']
        port = config['clickhouse']['Port']
        user = config['clickhouse']['User']
        password = config['clickhouse']['Pass']
        database = config['clickhouse']['Database']
        pool_size = int(config['clickhouse']['PoolSize'])
        # prepare pool
        self.clientPool = Queue(maxsize=pool_size)
        for i in range(0, pool_size):
            if len(user) > 0 and len(password) > 0:
                client = Client(host=host, port=port, user=user, password=password, database=database)
            else:
                client = Client(host=host, port=port, database=database)
            self.clientPool.put(client)

