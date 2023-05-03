#!/usr/bin/env python3

"""citus_arbitrary_configs
Usage:
    citus_arbitrary_configs --bindir=<bindir> --pgxsdir=<pgxsdir> --parallel=<parallel> --configs=<configs> --seed=<seed> [--base]

Options:
    --bindir=<bindir>              The PostgreSQL executable directory(ex: '~/.pgenv/pgsql-11.3/bin')
    --pgxsdir=<pgxsdir>           	       Path to the PGXS directory(ex: ~/.pgenv/src/postgresql-11.3)
    --parallel=<parallel>           how many configs to run in parallel
    --configs=<configs>     the config names to run
    --seed=<seed>                   random number seed
    --base                          whether to use the base sql schedule or not
"""
import concurrent.futures
import multiprocessing
import os
import random
import shutil
import sys
import time

from docopt import docopt

# https://stackoverflow.com/questions/14132789/relative-imports-for-the-billionth-time/14132912#14132912
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ignore E402 because these imports require addition to path
import common  # noqa: E402

import config as cfg  # noqa: E402

testResults = {}
parallel_thread_amount = 1


def _run_pg_regress_on_port(config, port, schedule_name, extra_tests=""):
    return common.run_pg_regress_without_exit(
        config.bindir,
        config.pg_srcdir,
        port,
        schedule_name,
        config.output_dir,
        config.input_dir,
        config.user,
        extra_tests,
    )


def run_for_config(config, lock, sql_schedule_name):
    name = config.name
    print("Running test for: {}".format(name))
    start_time = time.time()
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )
    if config.user == cfg.REGULAR_USER_NAME:
        common.create_role(
            config.bindir,
            config.node_name_to_ports.values(),
            config.user,
        )

    copy_copy_modified_binary(config.datadir)
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
    elif config.all_null_dist_key:
        exitCode |= common.run_pg_regress_without_exit(
            config.bindir,
            config.pg_srcdir,
            config.coordinator_port(),
            cfg.SINGLE_SHARD_PREP_SCHEDULE,
            config.output_dir,
            config.input_dir,
            cfg.SUPER_USER_NAME,
        )
        common.save_regression_diff(
            "single_shard_table_prep_regression", config.output_dir
        )

    exitCode |= _run_pg_regress_on_port(
        config, config.coordinator_port(), cfg.CREATE_SCHEDULE
    )
    common.save_regression_diff("create", config.output_dir)

    extra_tests = os.getenv("EXTRA_TESTS", "")
    if config.is_mx and config.worker_amount > 0:
        exitCode |= _run_pg_regress_on_port(
            config, config.random_port(), sql_schedule_name, extra_tests=extra_tests
        )
    else:
        exitCode |= _run_pg_regress_on_port(
            config,
            config.coordinator_port(),
            sql_schedule_name,
            extra_tests=extra_tests,
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


def copy_copy_modified_binary(datadir):
    shutil.copy("bin/copy_modified", datadir)
    shutil.copy("bin/copy_modified_wrapper", datadir)


def copy_test_files(config):
    sql_dir_path = os.path.join(config.datadir, "sql")
    expected_dir_path = os.path.join(config.datadir, "expected")

    common.initialize_temp_dir(sql_dir_path)
    common.initialize_temp_dir(expected_dir_path)
    for scheduleName in cfg.ARBITRARY_SCHEDULE_NAMES:
        with open(scheduleName) as file:
            lines = file.readlines()
            for line in lines:
                colon_index = line.find(":")
                # skip empty lines
                if colon_index == -1:
                    continue

                line = line[colon_index + 1 :].strip()
                test_names = line.split(" ")
                copy_test_files_with_names(
                    test_names, sql_dir_path, expected_dir_path, config
                )


def copy_test_files_with_names(test_names, sql_dir_path, expected_dir_path, config):
    for test_name in test_names:
        # make empty files for the skipped tests
        if test_name in config.skip_tests:
            expected_sql_file = os.path.join(sql_dir_path, test_name + ".sql")
            open(expected_sql_file, "x").close()

            expected_out_file = os.path.join(expected_dir_path, test_name + ".out")
            open(expected_out_file, "x").close()

            continue

        sql_name = os.path.join("./sql", test_name + ".sql")
        shutil.copy(sql_name, sql_dir_path)

        # for a test named <t>, all files:
        # <t>.out, <t>_0.out, <t>_1.out ...
        # are considered as valid outputs for the test
        # by the testing tool (pg_regress)
        # so copy such files to the testing directory
        output_name = os.path.join("./expected", test_name + ".out")
        alt_output_version_no = 0
        while os.path.isfile(output_name):
            # it might be the first time we run this test and the expected file
            # might not be there yet, in that case, we don't want to error out
            # while copying the file.
            shutil.copy(output_name, expected_dir_path)
            output_name = os.path.join(
                "./expected", f"{test_name}_{alt_output_version_no}.out"
            )
            alt_output_version_no += 1


def run_tests(configs, sql_schedule_name):
    failCount = 0
    common.initialize_temp_dir(cfg.CITUS_ARBITRARY_TEST_DIR)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=parallel_thread_amount
    ) as executor:
        manager = multiprocessing.Manager()
        lock = manager.Lock()
        futures = [
            executor.submit(run_for_config, config, lock, sql_schedule_name)
            for config in configs
        ]
        try:
            for future in futures:
                exitCode = future.result()
                if exitCode != 0:
                    failCount += 1
        except KeyboardInterrupt:
            exit(1)

    return failCount


def read_configs(docoptRes):
    configs = []
    # We fill the configs from all of the possible classes in config.py so that if we add a new config,
    # we don't need to add it here. And this avoids the problem where we forget to add it here
    for x in cfg.__dict__.values():
        if cfg.should_include_config(x):
            configs.append(x(docoptRes))
    return configs


def read_arguments(docoptRes):
    global parallel_thread_amount

    if "--parallel" in docoptRes and docoptRes["--parallel"] != "":
        parallel_thread_amount = int(docoptRes["--parallel"])

    seed = random.randint(1, 1000000)

    if "--seed" in docoptRes and docoptRes["--seed"] != "":
        seed = int(docoptRes["--seed"])

    random.seed(seed)

    configs = read_configs(docoptRes)
    if "--configs" in docoptRes and docoptRes["--configs"] != "":
        given_configs = docoptRes["--configs"].split(",")
        new_configs = []
        for config in configs:
            if config.name in given_configs:
                new_configs.append(config)
        if len(new_configs) > 0:
            configs = new_configs

    sql_schedule_name = cfg.SQL_SCHEDULE
    if "--base" in docoptRes and docoptRes["--base"]:
        sql_schedule_name = cfg.SQL_BASE_SCHEDULE
    return configs, sql_schedule_name, seed


def show_results(configs, testResults, runtime, seed):
    for testName, testResult in testResults.items():
        print("{}: {}".format(testName, testResult))

    print("--- {} seconds to run all tests! ---".format(end_time - start_time))
    print("---SEED: {} ---".format(seed))

    configCount = len(configs)
    if len(testResults) != configCount or failCount > 0:
        print(
            "actual {} expected {}, failCount: {}".format(
                len(testResults), configCount, failCount
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    docoptRes = docopt(__doc__)

    start_time = time.time()
    configs, sql_schedule_name, seed = read_arguments(docoptRes)
    failCount = run_tests(configs, sql_schedule_name)
    end_time = time.time()

    show_results(configs, testResults, end_time - start_time, seed)
