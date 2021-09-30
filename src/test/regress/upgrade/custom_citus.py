#!/usr/bin/env python3

"""custom_citus
Usage:
    custom_citus --bindir=<bindir> --pgxsdir=<pgxsdir> --parallel=<parallel>

Options:
    --bindir=<bindir>              The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --parallel=<parallel>           how many configs to run in parallel
"""

import upgrade_common as common
import threading
import atexit
from docopt import docopt
import os, shutil
import time
import sys

from config import (
    CitusDefaultClusterConfig, CitusSingleNodeClusterConfig,
    CitusSingleNodeSingleShardClusterConfig,
    CitusSingleShardClusterConfig,
    CitusMxClusterConfig,
    CitusManyShardsClusterConfig,
    CitusSingleNodeSingleSharedPoolSizeClusterConfig,
    CitusSingleNodeSingleConnectionClusterConfig,
    CitusSingleWorkerClusterConfig,
    CitusShardReplicationFactorClusterConfig,
    CitusNoLocalExecutionClusterConfig,
    CitusComplexClusterConfig,
    CitusSuperUserDefaultClusterConfig,
    CITUS_CUSTOM_TEST_DIR,
    CUSTOM_TEST_NAMES,
    COORDINATOR_NAME,
    CUSTOM_CREATE_SCHEDULE,
    CUSTOM_SQL_SCHEDULE,
    REGULAR_USER_NAME
)

testResults = {}
failCount = 0

def run_for_config(config, name):
    global failCount
    print ('Running test for: {}'.format(name))
    start_time = time.time()
    common.initialize_citus_cluster(config.bindir, config.datadir, config.settings, config)
    if config.user == REGULAR_USER_NAME:
        common.create_role(config.bindir, config.node_name_to_ports[COORDINATOR_NAME], 
            config.node_name_to_ports.values(), config.user)
    copy_test_files(config)

    exitCode = common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.node_name_to_ports[COORDINATOR_NAME], CUSTOM_CREATE_SCHEDULE, config.output_dir,
                   config.input_dir, config.user)
    common.save_regression_diff('create', config.output_dir)
    if config.is_mx:
        exitCode |= common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.random_worker_port(), CUSTOM_SQL_SCHEDULE, config.output_dir, config.input_dir, config.user)
    else:
        exitCode |= common.run_pg_regress_without_exit(config.bindir, config.pg_srcdir,
                   config.node_name_to_ports[COORDINATOR_NAME], CUSTOM_SQL_SCHEDULE, config.output_dir, config.input_dir, config.user)

    run_time = time.time() - start_time
    testResults[name] =  "SUCCESS" if exitCode == 0 else "FAIL: see {}".format(config.output_dir + '/run.out')
    testResults[name] += " runtime: {} seconds".format(run_time)

    if exitCode != 0:
        failCount += 1

    common.stop_databases(config.bindir, config.datadir, config.node_name_to_ports)
    common.save_regression_diff('sql', config.output_dir)

def copy_test_files(config):

    sql_dir_path = os.path.join(config.datadir, 'sql')
    expected_dir_path = os.path.join(config.datadir, 'expected')

    common.initialize_temp_dir(sql_dir_path)
    common.initialize_temp_dir(expected_dir_path)
    for test_name in CUSTOM_TEST_NAMES:
        sql_name = os.path.join('./sql', test_name + '.sql')
        output_name = os.path.join('./expected', test_name + '.out')
        shutil.copy(sql_name, sql_dir_path)
        shutil.copy(output_name, expected_dir_path)



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
        (CitusSingleWorkerClusterConfig(docoptRes), "citus single worker node cluster"),
        (CitusShardReplicationFactorClusterConfig(docoptRes), "citus with shard replication factor 2 cluster"),
        (CitusNoLocalExecutionClusterConfig(docoptRes), "citus with no local execution cluster"),
        (CitusComplexClusterConfig(docoptRes), "citus different settings cluster"),
        (CitusSuperUserDefaultClusterConfig(docoptRes), "citus super user default cluster"),
        
    ]

    start_time = time.time()


    parallel_thread_amount = 1
    if '--parallel' in docoptRes and docoptRes['--parallel'] != '':
        parallel_thread_amount = int(docoptRes['--parallel'])    

    testRunners = []
    common.initialize_temp_dir(CITUS_CUSTOM_TEST_DIR)
    for config, testName in configs:
        testRunner = TestRunner(config, testName)
        testRunner.start()
        testRunners.append(testRunner)
        if len(testRunners) >= parallel_thread_amount:
            for test in testRunners:
                test.join()
            testRunners = []

    for testRunner in testRunners:
        testRunner.join()

    for testName, testResult in testResults.items():
        print('{}: {}'.format(testName, testResult))

    end_time = time.time()
    print('--- {} seconds to run all tests! ---'.format(end_time - start_time))

    if len(testResults) != len(configs) or failCount > 0:
        sys.exit(1)
