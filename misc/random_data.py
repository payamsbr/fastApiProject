# create fake phone number and calls for clickhouse

import random
import time
import datetime
import configparser
from clickhouse_driver import Client

config = configparser.ConfigParser()
config.read('../config.ini')
host = config['clickhouse']['Ip']
port = config['clickhouse']['Port']
user = config['clickhouse']['User']
password = config['clickhouse']['Pass']
database = config['clickhouse']['Database']

if len(user) > 0 and len(password) > 0:
    client = Client(host=host, port=port, user=user, password=password, database=database)
else:
    client = Client(host=host, port=port, database=database)

# create table if not exists
client.execute('''CREATE TABLE IF NOT EXISTS call_samples
(
    id UInt64,
    call_id_a String,
    call_id_b String,
    duration UInt32,
    datetime DateTime
) ENGINE = TinyLog;''')

# prepare numbers lookup table
random_nums = []
for i in range(0, 20):
    n = random.randint(1000000, 9999999)
    random_nums.append('0912' + str(n))

# generate random data
max_i = 100
max_j = 100
datetime_ms = int(time.time() * 1000)
day = 1000 * 60 * 60 * 24
for i in range(0, max_i):
    query = 'INSERT INTO call_samples (id, call_id_a, call_id_b, duration, datetime) VALUES\n'
    sub_queries = []
    for j in range(0, max_j):
        duration = random.randint(1, 180)
        datetime_ms = datetime_ms + random.randint(int(.2*day), int(.6*day))
        datetime_str = datetime.datetime.fromtimestamp(datetime_ms/1000).strftime("%Y-%m-%d %H:%M:%S")
        nums = random.sample(random_nums, k=2)
        sub_queries.append(f'({i*max_j+j}, \'{nums[0]}\', \'{nums[1]}\', {duration}, \'{datetime_str}\')')
    query = query + (',\n'.join(sub_queries))
    client.execute(query)
    print(f'done {i} of {max_i}')
print('finished')
