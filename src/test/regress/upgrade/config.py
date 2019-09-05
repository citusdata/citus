BEFORE_UPGRADE_SCHEDULE = './before_upgrade_schedule'
AFTER_UPGRADE_SCHEDULE = './after_upgrade_schedule'


class Config():
    def __init__(self, arguments):
        self.old_bindir = arguments['--old-bindir']
        self.new_bindir = arguments['--new-bindir']
        self.pg_srcdir = arguments['--pgxsdir']
        self.temp_dir = './tmp_upgrade'
        self.old_datadir = self.temp_dir + '/oldData'
        self.new_datadir = self.temp_dir + '/newData'
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'prefer'
        }


USER = 'postgres'
DBNAME = 'postgres'

COORDINATOR_NAME = 'coordinator'
WORKER1 = 'worker1'
WORKER2 = 'worker2'
NODE_NAMES = [COORDINATOR_NAME, WORKER1, WORKER2]

WORKER_PORTS = [9701, 9702]
NODE_PORTS = {
    COORDINATOR_NAME: 9700,
    WORKER1: 9701,
    WORKER2: 9702,
}
