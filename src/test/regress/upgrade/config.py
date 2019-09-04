from os.path import expanduser


HOME = expanduser('~')
CURRENT_PG_PATH = HOME + '/.pgenv/pgsql/bin'
NEW_PG_PATH = HOME + '/.pgenv/pgsql-11.3/bin'
CITUS_PATH = HOME + "/citus"
USER = 'postgres'
DBNAME = 'postgres'
TEMP_DIR = CITUS_PATH + "/src/test/regress/tmp_upgrade"
NEW_PG_DATA_PATH = TEMP_DIR + '/newData'
CURRENT_PG_DATA_PATH = TEMP_DIR + '/oldData'
SCHEDULE_PATH = CITUS_PATH + '/src/test/regress'

PG_REGRESS_PATH = '~/.pgenv/src/postgresql-11.3/src/test/regress'
COORDINATOR_NAME = 'coordinator'
NODE_NAMES = [COORDINATOR_NAME, 'worker1', 'worker2']

WORKER_PORTS = [9701, 9702]
NODE_PORTS = {
    COORDINATOR_NAME: 9700,
    'worker1': 9701,
    'worker2': 9702,
}
