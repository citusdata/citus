from os.path import expanduser


HOME = expanduser('~')
CURRENT_PG_PATH = HOME + '/.pgenv/pgsql/bin'
CURRENT_PG_DATA_PATH = HOME + '/oldData'
NEW_PG_PATH = HOME + '/.pgenv/pgsql-11.3/bin'
NEW_PG_DATA_PATH = HOME + '/newData'

COORDINATOR_NAME = 'coordinator'
NODE_NAMES = [COORDINATOR_NAME, 'worker1', 'worker2']

WORKER_PORTS = [9701, 9702]
NODE_PORTS = {
    COORDINATOR_NAME: 9700,
    'worker1': 9701,
    'worker2': 9702,
}
