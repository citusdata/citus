BEFORE_UPGRADE_SCHEDULE = './before_pg_upgrade_schedule'
AFTER_UPGRADE_SCHEDULE = './after_pg_upgrade_schedule'

BEFORE_CITUS_UPGRADE_SCHEDULE = './before_citus{}_upgrade_schedule'
AFTER_CITUS_UPGRADE_SCHEDULE = './after_citus_upgrade_schedule'

class CitusUpgradeConfig():
    def __init__(self, arguments):
        self.bindir = arguments['--bindir']
        self.citus_version = arguments['--citus-version']
        self.pg_srcdir = arguments['--pgxsdir']
        self.pg_version = arguments['--pg-version']
        self.temp_dir = './tmp_citus_upgrade'
        self.datadir = self.temp_dir + '/data'
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'sslmode=prefer'
        }


class PGUpgradeConfig():
    def __init__(self, arguments):
        self.old_bindir = arguments['--old-bindir']
        self.new_bindir = arguments['--new-bindir']
        self.pg_srcdir = arguments['--pgxsdir']
        self.temp_dir = './tmp_upgrade'
        self.old_datadir = self.temp_dir + '/oldData'
        self.new_datadir = self.temp_dir + '/newData'
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'sslmode=prefer'
        }


USER = 'postgres'
DBNAME = 'postgres'

COORDINATOR_NAME = 'coordinator'
WORKER1 = 'worker1'
WORKER2 = 'worker2'
NODE_NAMES = [COORDINATOR_NAME, WORKER1, WORKER2]

WORKER_PORTS = [57636, 57637]
NODE_PORTS = {
    COORDINATOR_NAME: 57635,
    WORKER1: 57636,
    WORKER2: 57637,
}
