from os.path import expanduser
import upgrade_common as common
import random
import socket
from contextlib import closing


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


BEFORE_PG_UPGRADE_SCHEDULE = './before_pg_upgrade_schedule'
AFTER_PG_UPGRADE_SCHEDULE = './after_pg_upgrade_schedule'

CUSTOM_CREATE_SCHEDULE = './custom_create_schedule'
CUSTOM_SQL_SCHEDULE = './custom_sql_schedule'

AFTER_CITUS_UPGRADE_COORD_SCHEDULE = './after_citus_upgrade_coord_schedule'
BEFORE_CITUS_UPGRADE_COORD_SCHEDULE = './before_citus_upgrade_coord_schedule'
MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE = './mixed_before_citus_upgrade_schedule'
MIXED_AFTER_CITUS_UPGRADE_SCHEDULE = './mixed_after_citus_upgrade_schedule'

CITUS_CUSTOM_TEST_DIR = './tmp_citus_test'

MASTER = 'master'
# This should be updated when citus version changes
MASTER_VERSION = '10.2'

HOME = expanduser("~")


CITUS_VERSION_SQL = "SELECT extversion FROM pg_extension WHERE extname = 'citus';"


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

class NewInitCaller(type):
    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj.init()
        return obj

class CitusBaseClusterConfig(object, metaclass=NewInitCaller):

    data_dir_counter = 0

    def __init__(self, arguments):
        self.bindir = arguments['--bindir']
        self.pg_srcdir = arguments['--pgxsdir']
        self.temp_dir = CITUS_CUSTOM_TEST_DIR
        self.worker_amount = 2
        self.datadir = self.temp_dir + '/data'
        self.is_mx = False
        self.settings = {
            'shared_preload_libraries': 'citus',
            'citus.node_conninfo': 'sslmode=prefer',
        }

    def init(self):    
        self._init_node_name_ports()

        self.datadir += str(CitusBaseClusterConfig.data_dir_counter)
        CitusBaseClusterConfig.data_dir_counter += 1


    def setup_steps(self):
        pass

    def random_worker_port(self):
        return random.choice(self.worker_ports)

    def _init_node_name_ports(self):
        self.node_name_to_ports = {}
        self.worker_ports = []
        cur_port = self._get_and_update_next_port()
        self.node_name_to_ports[COORDINATOR_NAME] = cur_port
        for i in range(self.worker_amount):
            cur_port = self._get_and_update_next_port()
            cur_worker_name = 'worker{}'.format(i)
            self.node_name_to_ports[cur_worker_name] = cur_port
            self.worker_ports.append(cur_port)

    def _get_and_update_next_port(self):
        return find_free_port()

class CitusUpgradeConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        self.pre_tar_path = arguments['--citus-pre-tar']
        self.post_tar_path = arguments['--citus-post-tar']
        self.temp_dir = './tmp_citus_upgrade'
        self.new_settings = {
            'citus.enable_version_checks' : 'false'
        }
        self.mixed_mode = arguments['--mixed']
        self.settings.update(self.new_settings)



class CitusDefaultClusterConfig(CitusBaseClusterConfig):
    pass

class CitusSingleNodeClusterConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0

class CitusSingleWorkerClusterConfig(CitusBaseClusterConfig):

    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 1

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
        common.sync_metadata_to_workers(self.bindir, self.worker_ports, self.node_name_to_ports[COORDINATOR_NAME])

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

        

class PGUpgradeConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        self.old_bindir = arguments['--old-bindir']
        self.new_bindir = arguments['--new-bindir']
        self.temp_dir = './tmp_upgrade'
        self.old_datadir = self.temp_dir + '/oldData'
        self.new_datadir = self.temp_dir + '/newData'
