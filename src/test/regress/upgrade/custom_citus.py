#!/usr/bin/env python3

"""custom_citus
Usage:
    custom_citus --bindir=<bindir> --pgxsdir=<pgxsdir>

Options:
    --bindir=<bindir>              The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
"""

import upgrade_common as common
import threading
import atexit
from docopt import docopt

from config import (
    CitusDefaultClusterConfig, CitusSingleNodeClusterConfig,
    CitusSingleNodeSingleShardClusterConfig,
    CitusSingleShardClusterConfig,
    CitusMxClusterConfig,
    CitusManyShardsClusterConfig,
    CitusSingleNodeSingleSharedPoolSizeClusterConfig,
    CitusSingleNodeSingleConnectionClusterConfig,
    CitusSingleWorkerClusterConfig,
    CITUS_CUSTOM_TEST_DIR,
     USER, WORKER1PORT,
    NODE_NAMES, DBNAME, COORDINATOR_NAME,
    WORKER_PORTS, CUSTOM_CREATE_SCHEDULE,
    CUSTOM_SQL_SCHEDULE
)

testResults = {}        

def run_for_config(config, name):
    print ('Running test for: {}'.format(name))
    common.initialize_citus_cluster(config.bindir, config.datadir, config.settings, config)

    exitCode = common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.node_name_to_ports[COORDINATOR_NAME], CUSTOM_CREATE_SCHEDULE)
    if config.is_mx:    
        exitCode |= common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.random_worker_port(), CUSTOM_SQL_SCHEDULE)
    else:
        common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.node_name_to_ports[COORDINATOR_NAME], CUSTOM_SQL_SCHEDULE)                             
    testResults[name] =  "success" if exitCode == 0 else "fail"               
    common.stop_databases(config.bindir, config.datadir, config.node_name_to_ports)
    common.save_regression_diff(type(config).__name__)           



class TestRunner(threading.Thread):
    def __init__(self, config, name):
      threading.Thread.__init__(self)
      self.config = config
      self.name = name

    def run(self):
      run_for_config(self.config, self.name)


if __name__ == '__main__':
    docoptRes = docopt(__doc__)
    configs = [
        (CitusDefaultClusterConfig(docoptRes), "default citus cluster"),
        (CitusSingleNodeClusterConfig(docoptRes), "single node citus cluster"),
        (CitusSingleNodeSingleShardClusterConfig(docoptRes), "single node single shard cluster"),
        (CitusSingleShardClusterConfig(docoptRes), "single shard multiple workers cluster"),
        (CitusMxClusterConfig(docoptRes), "mx citus cluster"),
        (CitusManyShardsClusterConfig(docoptRes), "citus cluster with many shards"),
        (CitusSingleNodeSingleSharedPoolSizeClusterConfig(docoptRes), "citus single node single shared pool cluster"),
        (CitusSingleNodeSingleConnectionClusterConfig(docoptRes), "citus single node single connection cluster"),
        (CitusSingleWorkerClusterConfig(docoptRes), "citus single worker node cluster")


    ]

    testRunners = []
    common.initialize_temp_dir(CITUS_CUSTOM_TEST_DIR)
    for config, testName in configs:
        testRunner = TestRunner(config, testName)
        testRunner.start()
        testRunners.append(testRunner)

    for testRunner in testRunners:
        testRunner.join()

    for testName, testResult in testResults.items():
        print('{}: {}'.format(testName, testResult))


