from os.path import expanduser
import random
import socket
from contextlib import closing
import os
import threading
import common
import inspect

COORDINATOR_NAME = "coordinator"
WORKER1 = "worker1"
WORKER2 = "worker2"

REGULAR_USER_NAME = "regularuser"
SUPER_USER_NAME = "postgres"

ARBITRARY_SCHEDULE_NAMES = [
    "create_schedule",
    "sql_schedule",
    "sql_base_schedule",
    "postgres_schedule",
]

BEFORE_PG_UPGRADE_SCHEDULE = "./before_pg_upgrade_schedule"
AFTER_PG_UPGRADE_SCHEDULE = "./after_pg_upgrade_schedule"

CREATE_SCHEDULE = "./create_schedule"
POSTGRES_SCHEDULE = "./postgres_schedule"
SQL_SCHEDULE = "./sql_schedule"
SQL_BASE_SCHEDULE = "./sql_base_schedule"

AFTER_CITUS_UPGRADE_COORD_SCHEDULE = "./after_citus_upgrade_coord_schedule"
BEFORE_CITUS_UPGRADE_COORD_SCHEDULE = "./before_citus_upgrade_coord_schedule"
MIXED_BEFORE_CITUS_UPGRADE_SCHEDULE = "./mixed_before_citus_upgrade_schedule"
MIXED_AFTER_CITUS_UPGRADE_SCHEDULE = "./mixed_after_citus_upgrade_schedule"

CITUS_ARBITRARY_TEST_DIR = "./tmp_citus_test"

MASTER = "master"
# This should be updated when citus version changes
MASTER_VERSION = "11.0"

HOME = expanduser("~")


CITUS_VERSION_SQL = "SELECT extversion FROM pg_extension WHERE extname = 'citus';"


# this is out of ephemeral port range for many systems hence
# it is a lower change that it will conflict with "in-use" ports
next_port = 10200

# ephemeral port start on many linux systems
PORT_UPPER = 32768

port_lock = threading.Lock()


def should_include_config(class_name):

    if inspect.isclass(class_name) and (
        issubclass(class_name, CitusMXBaseClusterConfig)
        or issubclass(class_name, CitusDefaultClusterConfig)
    ):
        return True
    return False


def find_free_port():
    global next_port
    with port_lock:
        while next_port < PORT_UPPER:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                try:
                    s.bind(("localhost", next_port))
                    port = next_port
                    next_port += 1
                    return port
                except:
                    next_port += 1
    # we couldn't find a port
    raise Exception("Couldn't find a port to use")


class NewInitCaller(type):
    def __call__(cls, *args, **kwargs):
        obj = type.__call__(cls, *args, **kwargs)
        obj.post_init()
        return obj


class CitusBaseClusterConfig(object, metaclass=NewInitCaller):
    def __init__(self, arguments):
        if "--bindir" in arguments:
            self.bindir = arguments["--bindir"]
        self.pg_srcdir = arguments["--pgxsdir"]
        self.temp_dir = CITUS_ARBITRARY_TEST_DIR
        self.worker_amount = 2
        self.user = REGULAR_USER_NAME
        self.is_mx = False
        self.is_citus = True
        self.name = type(self).__name__
        self.settings = {
            "shared_preload_libraries": "citus",
            "log_error_verbosity": "terse",
            "citus.node_conninfo": "sslmode=prefer",
            "citus.enable_repartition_joins": True,
            "citus.repartition_join_bucket_count_per_node": 2,
            "max_connections": 600,
        }
        self.new_settings = {}
        self.add_coordinator_to_metadata = False

    def post_init(self):
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

    def random_port(self):
        return random.choice(list(self.node_name_to_ports.values()))

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


class CitusDefaultClusterConfig(CitusBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        new_settings = {
            "client_min_messages": "WARNING",
            "citus.sort_returning": True,
        }
        self.settings.update(new_settings)
        self.add_coordinator_to_metadata = True


class CitusMXBaseClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.is_mx = True


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


class PostgresConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0
        self.is_citus = False


class CitusSingleNodeClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0

    def setup_steps(self):
        common.coordinator_should_haveshards(self.bindir, self.coordinator_port())


class CitusSingleWorkerClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 1


class CitusSuperUserDefaultClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.user = SUPER_USER_NAME


class CitusThreeWorkersManyShardsClusterConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {"citus.shard_count": 191}
        self.worker_amount = 3

    def setup_steps(self):
        common.coordinator_should_haveshards(self.bindir, self.coordinator_port())


class CitusSmallSharedPoolSizeConfig(CitusMXBaseClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.new_settings = {
            # can be uncommented after https://github.com/citusdata/citus/issues/5342
            # "citus.local_shared_pool_size": 5,
            "citus.max_shared_pool_size": 5,
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
            # https://github.com/citusdata/citus/issues/5345
            # "citus.force_max_query_parallelization": "on",
            "citus.binary_worker_copy_format": False,
            "citus.enable_binary_protocol": False,
            "citus.local_table_join_policy": "prefer-distributed",
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
            "citus.values_materialization_threshold": "0",
        }


class CitusSingleNodeSingleShardClusterConfig(CitusDefaultClusterConfig):
    def __init__(self, arguments):
        super().__init__(arguments)
        self.worker_amount = 0
        self.new_settings = {"citus.shard_count": 1}

    def setup_steps(self):
        common.coordinator_should_haveshards(self.bindir, self.coordinator_port())


class CitusShardReplicationFactorClusterConfig(CitusMXBaseClusterConfig):
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
        self.add_coordinator_to_metadata = True
