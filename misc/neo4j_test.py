import neo4j.exceptions
from neo4j import GraphDatabase, Session, ManagedTransaction
import configparser

# read configurations
config = configparser.ConfigParser()
config.read('../config.ini')
host = config['neo4j']['Ip']
port = config['neo4j']['Port']
user = config['neo4j']['User']
password = config['neo4j']['Pass']
pool_size = int(config['neo4j']['PoolSize'])
uri = f"bolt://{host}:{port}"

# prepare database
driver = GraphDatabase.driver(uri, auth=(user, password))
session = driver.session()

def _transaction_func(tx: ManagedTransaction):
    result = tx.run("MATCH (c:Person) return c")
    print('done')
    print(result)
    records = list(result)
    summary = result.consume()
    print(records)
    print(summary)


session.execute_read(_transaction_func)
