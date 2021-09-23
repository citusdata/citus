from os.path import expanduser
import upgrade_common as common


BEFORE_PG_UPGRADE_SCHEDULE = './before_pg_upgrade_schedule'
AFTER_PG_UPGRADE_SCHEDULE = './after_pg_upgrade_schedule'

CUSTOM_CREATE_SCHEDULE = './custom_create_schedule'
CUSTOM_SQL_SCHEDULE = './custom_sql_schedule'

AFTER_CITUS_UPGRADE_COORD_SCHEDULE = './after_citus_upgrade_coord_schedule'
BEFORE_CITUS_UPGRADE_COORD_SCHEDULE = './before_citus_upgrade_coord_schedule'
MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE = './mixed_before_citus_upgrade_schedule'
MIXED_AFTER_CITUS_UPGRADE_SCHEDULE = './mixed_after_citus_upgrade_schedule'

MASTER = 'master'
# This should be updated when citus version changes
MASTER_VERSION = '10.2'

HOME = expanduser("~")


CITUS_VERSION_SQL = "SELECT extversion FROM pg_extension WHERE extname = 'citus';"


class CitusUpgradeConfig():

    def __init__(self, arguments):
        self.bindir = arguments['--bindir']
        self.pre_tar_path = arguments['--citus-pre-tar']
        self.post_tar_path = arguments['--citus-post-tar']
        self.pg_srcdir = arguments['--pgxsdir']
        self.worker_amount = 2
        self.temp_dir = './tmp_citus_upgrade'
        self.datadir = self.temp_dir + '/data'
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'sslmode=prefer',
            'citus.enable_version_checks' : 'false'
        }
        self.mixed_mode = arguments['--mixed']


class CitusBaseClusterConfig():
    def __init__(self, arguments):
        self.bindir = arguments['--bindir']
        self.pg_srcdir = arguments['--pgxsdir']
        self.temp_dir = './tmp_citus_test'
        self.worker_amount = 2
        self.datadir = self.temp_dir + '/data'
        self.is_mx = False
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'sslmode=prefer',
        }

    def setup_steps(self):
        pass

class CitusDefaultClusterConfig(CitusBaseClusterConfig):
    pass

class CitusSingleNodeClusterConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0


class CitusSingleNodeSingleShardClusterConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0
        self.new_settings = {
            'citus.shard_count': 1
        }
        self.settings.update(self.new_settings)

class CitusSingleShardClusterConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            'citus.shard_count': 1
        }
        self.settings.update(self.new_settings)

class CitusMxClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.is_mx = True

    def setup_steps(self):
        common.sync_metadata_to_workers(self.bindir)

class CitusManyShardsClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            'citus.shard_count': 500
        }
        self.settings.update(self.new_settings)

class CitusSingleNodeSingleConnectionClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            'citus.max_adaptive_executor_pool_size': 1
        }
        self.settings.update(self.new_settings)

class CitusSingleNodeSingleSharedPoolSizeClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            'citus.max_shared_pool_size': 1
        }
        self.settings.update(self.new_settings)

        

class PGUpgradeConfig():
    def __init__(self, arguments):
        self.old_bindir = arguments['--old-bindir']
        self.new_bindir = arguments['--new-bindir']
        self.pg_srcdir = arguments['--pgxsdir']
        self.temp_dir = './tmp_upgrade'
        self.old_datadir = self.temp_dir + '/oldData'
        self.new_datadir = self.temp_dir + '/newData'
        self.worker_amount = 2

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
COORDINATOR_PORT = 57635
WORKER1PORT = 57636
WORKER2PORT = 57637

WORKER_PORTS = [WORKER1PORT, WORKER2PORT]
NODE_PORTS = {
    COORDINATOR_NAME: COORDINATOR_PORT,
    WORKER1: WORKER1PORT,
    WORKER2: WORKER2PORT,
}
