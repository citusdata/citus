#!/usr/bin/env python3

"""citus_arbitrary_configs
Usage:
    citus_arbitrary_configs --bindir=<bindir> --pgxsdir=<pgxsdir> --parallel=<parallel> --extra-tests=<extra_tests> --seed=<seed>

Options:
    --bindir=<bindir>              The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --parallel=<parallel>           how many configs to run in parallel
    --extra-tests=<extra-tests>     the config names to run
    --seed=<seed>                   random number seed
"""
import sys
import os, shutil

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import common
import config as cfg
import concurrent.futures
import multiprocessing
from docopt import docopt
import time
import inspect
import random


testResults = {}


def run_for_config(config, lock):
    name = config.name
    print("Running test for: {}".format(name))
    start_time = time.time()
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )
    if config.user == cfg.REGULAR_USER_NAME:
        common.create_role(
            config.bindir,
            config.coordinator_port(),
            config.node_name_to_ports.values(),
            config.user,
        )
    copy_test_files(config)

    exitCode = 0
    if not config.is_citus:
        exitCode |= common.run_pg_regress_without_exit(
            config.bindir,
            config.pg_srcdir,
            config.coordinator_port(),
            cfg.POSTGRES_SCHEDULE,
            config.output_dir,
            config.input_dir,
            cfg.SUPER_USER_NAME,
        )
        common.save_regression_diff("postgres", config.output_dir)

    exitCode |= common.run_pg_regress_without_exit(
        config.bindir,
        config.pg_srcdir,
        config.coordinator_port(),
        cfg.CREATE_SCHEDULE,
        config.output_dir,
        config.input_dir,
        config.user,
    )
    common.save_regression_diff("create", config.output_dir)

    if config.is_mx and config.worker_amount > 0:
        exitCode |= common.run_pg_regress_without_exit(
            config.bindir,
            config.pg_srcdir,
            config.random_port(),
            cfg.SQL_SCHEDULE,
            config.output_dir,
            config.input_dir,
            config.user,
        )
    else:
        exitCode |= common.run_pg_regress_without_exit(
            config.bindir,
            config.pg_srcdir,
            config.coordinator_port(),
            cfg.SQL_SCHEDULE,
            config.output_dir,
            config.input_dir,
            config.user,
        )

    run_time = time.time() - start_time
    with lock:
        testResults[name] = (
            "SUCCESS"
            if exitCode == 0
            else "FAIL: see {}".format(config.output_dir + "/run.out")
        )
        testResults[name] += " runtime: {} seconds".format(run_time)

    common.stop_databases(
        config.bindir, config.datadir, config.node_name_to_ports, config.name
    )
    common.save_regression_diff("sql", config.output_dir)
    return exitCode


def copy_test_files(config):

    sql_dir_path = os.path.join(config.datadir, "sql")
    expected_dir_path = os.path.join(config.datadir, "expected")

    common.initialize_temp_dir(sql_dir_path)
    common.initialize_temp_dir(expected_dir_path)
    for scheduleName in cfg.ARBITRARY_SCHEDULE_NAMES:
        with open(scheduleName) as file:
            lines = file.readlines()
            for line in lines:
                colon_index = line.index(":")
                line = line[colon_index + 1 :].strip()
                test_names = line.split(" ")
                copy_test_files_with_names(test_names, sql_dir_path, expected_dir_path)


def copy_test_files_with_names(test_names, sql_dir_path, expected_dir_path):
    for test_name in test_names:
        sql_name = os.path.join("./sql", test_name + ".sql")
        output_name = os.path.join("./expected", test_name + ".out")
        shutil.copy(sql_name, sql_dir_path)
        if os.path.isfile(output_name):
            # it might be the first time we run this test and the expected file
            # might not be there yet, in that case, we don't want to error out
            # while copying the file.
            shutil.copy(output_name, expected_dir_path)


def read_configs(docoptRes):
    configs = []
    # We fill the configs from all of the possible classes in config.py so that if we add a new config,
    # we don't need to add it here. And this avoids the problem where we forget to add it here
    for x in cfg.__dict__.values():
        if inspect.isclass(x) and (
            issubclass(x, cfg.CitusMXBaseClusterConfig)
            or issubclass(x, cfg.CitusDefaultClusterConfig)
        ):
            configs.append(x(docoptRes))
    return configs


def run_tests(configs):
    failCount = 0
    common.initialize_temp_dir(cfg.CITUS_ARBITRARY_TEST_DIR)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=parallel_thread_amount
    ) as executor:
        manager = multiprocessing.Manager()
        lock = manager.Lock()
        futures = [executor.submit(run_for_config, config, lock) for config in configs]
        for future in futures:
            exitCode = future.result()
            if exitCode != 0:
                failCount += 1

    return failCount


if __name__ == "__main__":
    docoptRes = docopt(__doc__)

    start_time = time.time()

    parallel_thread_amount = 1
    if "--parallel" in docoptRes and docoptRes["--parallel"] != "":
        parallel_thread_amount = int(docoptRes["--parallel"])

    seed = random.randint(1, 1000000)

    if "--seed" in docoptRes and docoptRes["--seed"] != "":
        seed = int(docoptRes["--seed"])

    random.seed(seed)

    configs = read_configs(docoptRes)
    if "--extra-tests" in docoptRes and docoptRes["--extra-tests"] != "":
        extra_tests = docoptRes["--extra-tests"].split(",")
        new_configs = []
        for config in configs:
            if config.name in extra_tests:
                new_configs.append(config)
        if len(new_configs) > 0:
            configs = new_configs

    failCount = run_tests(configs)

    for testName, testResult in testResults.items():
        print("{}: {}".format(testName, testResult))

    end_time = time.time()
    print("--- {} seconds to run all tests! ---".format(end_time - start_time))
    print("---SEED: {} ---".format(seed))
    if len(testResults) != len(configs) or failCount > 0:
        print(
            "actual {} expected {}, failCount: {}".format(
                len(testResults), len(configs), failCount
            )
        )
        sys.exit(1)
