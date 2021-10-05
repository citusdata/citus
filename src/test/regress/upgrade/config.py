from os.path import expanduser
import random
import socket
from contextlib import closing
import os
import upgrade_common as common

COORDINATOR_NAME = "coordinator"
WORKER1 = "worker1"
WORKER2 = "worker2"

REGULAR_USER_NAME = "regularuser"
SUPER_USER_NAME = "postgres"

CUSTOM_SCHEDULE_NAMES = [
    "custom_create_schedule",
    "custom_sql_schedule",
    "custom_postgres_schedule",
]

BEFORE_PG_UPGRADE_SCHEDULE = "./before_pg_upgrade_schedule"
AFTER_PG_UPGRADE_SCHEDULE = "./after_pg_upgrade_schedule"

CUSTOM_CREATE_SCHEDULE = "./custom_create_schedule"
CUSTOM_POSTGRES_SCHEDULE = "./custom_postgres_schedule"
CUSTOM_SQL_SCHEDULE = "./custom_sql_schedule"

AFTER_CITUS_UPGRADE_COORD_SCHEDULE = "./after_citus_upgrade_coord_schedule"
BEFORE_CITUS_UPGRADE_COORD_SCHEDULE = "./before_citus_upgrade_coord_schedule"
MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE = "./mixed_before_citus_upgrade_schedule"
MIXED_AFTER_CITUS_UPGRADE_SCHEDULE = "./mixed_after_citus_upgrade_schedule"

CITUS_CUSTOM_TEST_DIR = "./tmp_citus_test"

MASTER = "master"
# This should be updated when citus version changes
MASTER_VERSION = "10.2"

HOME = expanduser("~")


CITUS_VERSION_SQL = "SELECT extversion FROM pg_extension WHERE extname = 'citus';"


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class NewInitCaller(type):
    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj.init()
        return obj


class CitusBaseClusterConfig(object, metaclass=NewInitCaller):
    def __init__(self, arguments):
        if "--bindir" in arguments:
            self.bindir = arguments["--bindir"]
        self.pg_srcdir = arguments["--pgxsdir"]
        self.temp_dir = CITUS_CUSTOM_TEST_DIR
        self.worker_amount = 2
        self.user = REGULAR_USER_NAME
        self.is_mx = False
        self.is_citus = True
        self.name = type(self).__name__
        self.settings = {
            "shared_preload_libraries": "citus",
            "citus.node_conninfo": "sslmode=prefer",
            "citus.enable_repartition_joins": True,
        }
        self.new_settings = {}
        self.add_coordinator_to_metadata = False

    def init(self):
        self._init_node_name_ports()

        self.datadir = self.temp_dir + "/data"
        self.datadir += self.name
        self.input_dir = self.datadir
        self.output_dir = self.datadir
        self.output_file = os.path.join(self.datadir, "run.out")
        if self.worker_amount > 0:
            self.chosen_random_worker_port = self.random_worker_port()
        self.settings.update(self.new_settings)

    def coordinator_port(self):
        return self.node_name_to_ports[COORDINATOR_NAME]

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
            cur_worker_name = "worker{}".format(i)
            self.node_name_to_ports[cur_worker_name] = cur_port
            self.worker_ports.append(cur_port)

    def _get_and_update_next_port(self):
        if hasattr(self, "fixed_port"):
            next_port = self.fixed_port
            self.fixed_port += 1
            return next_port
        return find_free_port()


class CitusMXBaseClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.is_mx = True
        self.add_coordinator_to_metadata = True


class CitusUpgradeConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.pre_tar_path = arguments["--citus-pre-tar"]
        self.post_tar_path = arguments["--citus-post-tar"]
        self.temp_dir = "./tmp_citus_upgrade"
        self.new_settings = {"citus.enable_version_checks": "false"}
        self.user = SUPER_USER_NAME
        self.mixed_mode = arguments["--mixed"]
        self.fixed_port = 57635


class CitusDefaultClusterConfig(CitusBaseClusterConfig):
    pass


class PostgresConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0
        self.is_citus = False


class CitusSingleNodeClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0


class CitusSingleWorkerClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 1


class CitusSuperUserDefaultClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.user = SUPER_USER_NAME


class CitusFiveWorkersManyShardsClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {"citus.shard_count": 494}
        self.worker_amount = 5

    def setup_steps(self):
        common.coordinator_should_haveshards(self.bindir, self.coordinator_port())


class CitusSmallSharedPoolSizeConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.local_shared_pool_size": 2,
            "citus.max_shared_pool_size": 2,
        }


class CitusSmallExecutorPoolSizeConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.max_adaptive_executor_pool_size": 2,
        }


class CitusSequentialExecutionConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.multi_shard_modify_mode": "sequential",
        }


class CitusCacheManyConnectionsConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.max_cached_conns_per_worker": 4,
        }


class CitusUnusualExecutorConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.max_adaptive_executor_pool_size": 7,
            "citus.executor_slow_start_interval": 1,
            "citus.prevent_incomplete_connection_establishment": False,
            "citus.enable_cost_based_connection_establishment": False,
            "citus.max_cached_connection_lifetime": "10ms",
            "citus.force_max_query_parallelization": "on",
            "citus.binary_worker_copy_format": False,
            "citus.enable_binary_protocol": False,
        }


class CitusCacheManyConnectionsConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.copy_switchover_threshold": "1B",
            "citus.local_copy_flush_threshold": "1B",
            "citus.remote_copy_flush_threshold": "1B",
        }


class CitusSmallCopyBuffersConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.copy_switchover_threshold": "1B",
            "citus.local_copy_flush_threshold": "1B",
            "citus.remote_copy_flush_threshold": "1B",
        }


class CitusUnusualQuerySettingsConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            "citus.task_assignment_policy": "first-replica",
            "citus.enable_fast_path_router_planner": False,
            "citus.enable_local_execution": False,
            "citus.enable_single_hash_repartition_joins": True,
            "citus.recover_2pc_interval": "1s",
            "citus.remote_task_check_interval": "1ms",
        }


class CitusSingleNodeSingleShardClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0
        self.new_settings = {"citus.shard_count": 1}


class CitusShardReplicationFactorClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {"citus.shard_replication_factor": 2}


class CitusSingleShardClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {"citus.shard_count": 1}


class CitusNonMxClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.is_mx = False


class PGUpgradeConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.old_bindir = arguments["--old-bindir"]
        self.new_bindir = arguments["--new-bindir"]
        self.temp_dir = "./tmp_upgrade"
        self.old_datadir = self.temp_dir + "/oldData"
        self.new_datadir = self.temp_dir + "/newData"
        self.user = SUPER_USER_NAME
