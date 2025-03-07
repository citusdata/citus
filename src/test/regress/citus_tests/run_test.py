#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import pathlib
import random
import re
import shutil
import sys
from collections import OrderedDict
from contextlib import contextmanager
from typing import Optional

import common
from common import OLDEST_SUPPORTED_CITUS_VERSION, PG_BINDIR, REGRESS_DIR, capture, run
from upgrade import generate_citus_tarball, run_citus_upgrade_tests

from config import ARBITRARY_SCHEDULE_NAMES, CitusBaseClusterConfig, CitusUpgradeConfig


def main():
    args = parse_arguments()

    test_name = get_test_name(args)

    # All python tests start with test_ and all other tests don't. This is by
    # convention.
    if test_name.startswith("test_"):
        run_python_test(test_name, args)
        # above function never returns
    else:
        run_regress_test(test_name, args)


def parse_arguments():
    args = argparse.ArgumentParser()
    args.add_argument(
        "test_name", help="Test name (must be included in a schedule.)", nargs="?"
    )
    args.add_argument(
        "-p",
        "--path",
        required=False,
        help="Relative path for test file (must have a .sql or .spec extension)",
        type=pathlib.Path,
    )
    args.add_argument(
        "-r", "--repeat", help="Number of test to run", type=int, default=1
    )
    args.add_argument(
        "-b",
        "--use-base-schedule",
        required=False,
        help="Choose base-schedules rather than minimal-schedules",
        action="store_true",
    )
    args.add_argument(
        "-w",
        "--use-whole-schedule-line",
        required=False,
        help="Use the whole line found in related schedule",
        action="store_true",
    )
    args.add_argument(
        "--valgrind",
        required=False,
        help="Run the test with valgrind enabled",
        action="store_true",
    )

    return vars(args.parse_args())


class TestDeps:
    schedule: Optional[str]
    direct_extra_tests: list[str]

    def __init__(
        self,
        schedule,
        extra_tests=None,
        repeatable=True,
        worker_count=2,
        citus_upgrade_infra=False,
    ):
        self.schedule = schedule
        self.direct_extra_tests = extra_tests or []
        self.repeatable = repeatable
        self.worker_count = worker_count
        self.citus_upgrade_infra = citus_upgrade_infra

    def extra_tests(self):
        all_deps = OrderedDict()
        for direct_dep in self.direct_extra_tests:
            if direct_dep in DEPS:
                for indirect_dep in DEPS[direct_dep].extra_tests():
                    all_deps[indirect_dep] = True
            all_deps[direct_dep] = True

        return list(all_deps.keys())


DEPS = {
    "multi_cluster_management": TestDeps(
        None, ["multi_test_helpers_superuser"], repeatable=False
    ),
    "minimal_cluster_management": TestDeps(
        None, ["multi_test_helpers_superuser"], repeatable=False
    ),
    "create_role_propagation": TestDeps(None, ["multi_cluster_management"]),
    "single_node_enterprise": TestDeps(None),
    "single_node": TestDeps(None, ["multi_test_helpers"]),
    "single_node_truncate": TestDeps(None),
    "multi_explain": TestDeps(
        "base_schedule", ["multi_insert_select_non_pushable_queries"]
    ),
    "multi_extension": TestDeps(None, repeatable=False),
    "multi_test_helpers": TestDeps(None),
    "multi_insert_select": TestDeps("base_schedule"),
    "multi_partitioning": TestDeps("base_schedule"),
    "multi_mx_create_table": TestDeps(
        None,
        [
            "multi_test_helpers_superuser",
            "multi_mx_node_metadata",
            "multi_cluster_management",
            "multi_mx_function_table_reference",
        ],
    ),
    "alter_distributed_table": TestDeps(
        "minimal_schedule", ["multi_behavioral_analytics_create_table"]
    ),
    "background_rebalance": TestDeps(
        None,
        [
            "multi_test_helpers",
            "multi_cluster_management",
        ],
        worker_count=3,
    ),
    "background_rebalance_parallel": TestDeps(
        None,
        [
            "multi_test_helpers",
            "multi_cluster_management",
        ],
        worker_count=6,
    ),
    "function_propagation": TestDeps("minimal_schedule"),
    "grant_on_foreign_server_propagation": TestDeps("minimal_schedule"),
    "multi_mx_modifying_xacts": TestDeps(None, ["multi_mx_create_table"]),
    "multi_mx_router_planner": TestDeps(None, ["multi_mx_create_table"]),
    "multi_mx_copy_data": TestDeps(None, ["multi_mx_create_table"]),
    "multi_mx_schema_support": TestDeps(None, ["multi_mx_copy_data"]),
    "multi_simple_queries": TestDeps("base_schedule"),
    "create_single_shard_table": TestDeps("minimal_schedule"),
    "isolation_extension_commands": TestDeps(
        None, ["isolation_setup", "isolation_add_remove_node"]
    ),
    "isolation_update_node": TestDeps(
        None, ["isolation_setup", "isolation_add_remove_node"]
    ),
    "schema_based_sharding": TestDeps("minimal_schedule"),
    "multi_sequence_default": TestDeps(
        None,
        [
            "multi_test_helpers",
            "multi_cluster_management",
            "multi_table_ddl",
        ],
    ),
    "grant_on_schema_propagation": TestDeps("minimal_schedule"),
    "propagate_extension_commands": TestDeps("minimal_schedule"),
    "multi_size_queries": TestDeps("base_schedule", ["multi_copy"]),
    "multi_mx_node_metadata": TestDeps(
        None,
        [
            "multi_extension",
            "multi_test_helpers",
            "multi_test_helpers_superuser",
        ],
    ),
    "foreign_key_to_reference_shard_rebalance": TestDeps(
        "minimal_schedule", ["remove_coordinator_from_metadata"]
    ),
    "limit_intermediate_size": TestDeps("base_schedule"),
}


def run_python_test(test_name, args):
    """Runs the test using pytest

    This function never returns as it usese os.execlp to replace the current
    process with a new pytest process.
    """
    test_path = REGRESS_DIR / "citus_tests" / "test" / f"{test_name}.py"
    if not test_path.exists():
        raise Exception("Test could not be found in any schedule")

    os.execlp(
        "pytest",
        "pytest",
        "--numprocesses",
        "auto",
        "--count",
        str(args["repeat"]),
        str(test_path),
    )


def run_regress_test(test_name, args):
    original_schedule, schedule_line = find_test_schedule_and_line(test_name, args)
    print(f"SCHEDULE: {original_schedule}")
    print(f"SCHEDULE_LINE: {schedule_line}")

    dependencies = test_dependencies(test_name, original_schedule, schedule_line, args)

    with tmp_schedule(test_name, dependencies, schedule_line, args) as schedule:
        if "upgrade" in original_schedule:
            run_schedule_with_python(test_name, schedule, dependencies)
        else:
            run_schedule_with_multiregress(test_name, schedule, dependencies, args)


def run_schedule_with_python(test_name, schedule, dependencies):
    pgxs_path = pathlib.Path(capture("pg_config --pgxs").rstrip())

    os.chdir(REGRESS_DIR)
    os.environ["PATH"] = str(REGRESS_DIR / "bin") + os.pathsep + os.environ["PATH"]
    os.environ["PG_REGRESS_DIFF_OPTS"] = "-dU10 -w"

    fake_config_args = {
        "--pgxsdir": str(pgxs_path.parent.parent.parent),
        "--bindir": PG_BINDIR,
        "--mixed": False,
    }

    if dependencies.citus_upgrade_infra:
        run_single_citus_upgrade_test(test_name, schedule, fake_config_args)
        return

    config = CitusBaseClusterConfig(fake_config_args)
    common.initialize_temp_dir(config.temp_dir)
    common.initialize_citus_cluster(
        config.bindir, config.datadir, config.settings, config
    )
    common.run_pg_regress(
        config.bindir, config.pg_srcdir, config.coordinator_port(), schedule
    )


def run_single_citus_upgrade_test(test_name, schedule, fake_config_args):
    os.environ["CITUS_OLD_VERSION"] = f"v{OLDEST_SUPPORTED_CITUS_VERSION}"
    citus_tarball_path = generate_citus_tarball(f"v{OLDEST_SUPPORTED_CITUS_VERSION}")
    config = CitusUpgradeConfig(fake_config_args, citus_tarball_path, None)

    # Before tests are a simple case, because no actual upgrade is needed
    if "_before" in test_name:
        run_citus_upgrade_tests(config, schedule, None)
        return

    before_schedule_name = f"{schedule}_before"
    before_schedule_path = REGRESS_DIR / before_schedule_name

    before_test_name = test_name.replace("_after", "_before")
    with open(before_schedule_path, "w") as before_schedule_file:
        before_schedule_file.write(f"test: {before_test_name}\n")

    try:
        run_citus_upgrade_tests(config, before_schedule_name, schedule)
    finally:
        os.remove(before_schedule_path)


def run_schedule_with_multiregress(test_name, schedule, dependencies, args):
    worker_count = needed_worker_count(test_name, dependencies)

    # find suitable make recipe
    if dependencies.schedule == "base_isolation_schedule" or "isolation" in test_name:
        make_recipe = "check-isolation-custom-schedule"
    elif dependencies.schedule == "failure_base_schedule" or "failure" in test_name:
        make_recipe = "check-failure-custom-schedule"
    else:
        make_recipe = "check-custom-schedule"

    if args["valgrind"]:
        make_recipe += "-vg"

    # prepare command to run tests
    test_command = (
        f"make -C {REGRESS_DIR} {make_recipe} "
        f"WORKERCOUNT={worker_count} "
        f"SCHEDULE='{schedule}'"
    )
    run(test_command)


def default_base_schedule(test_schedule, args):
    if "isolation" in test_schedule:
        return "base_isolation_schedule"

    if "failure" in test_schedule:
        return "failure_base_schedule"

    if "enterprise" in test_schedule:
        return "enterprise_minimal_schedule"

    if "split" in test_schedule:
        return "minimal_schedule"

    if "mx" in test_schedule:
        if args["use_base_schedule"]:
            return "mx_base_schedule"
        return "mx_minimal_schedule"

    if "operations" in test_schedule:
        return "minimal_schedule"

    if "pg_upgrade" in test_schedule:
        return "minimal_pg_upgrade_schedule"

    if test_schedule in ARBITRARY_SCHEDULE_NAMES:
        print(f"WARNING: Arbitrary config schedule ({test_schedule}) is not supported.")
        sys.exit(0)

    if args["use_base_schedule"]:
        return "base_schedule"
    return "minimal_schedule"


# we run the tests with 2 workers by default.
# If we find any dependency which requires more workers, we update the worker count.
def worker_count_for(test_name):
    if test_name in DEPS:
        return DEPS[test_name].worker_count
    return 2


def get_test_name(args):
    if args["test_name"]:
        return args["test_name"]

    if not args["path"]:
        print("FATAL: No test given.")
        sys.exit(2)

    absolute_test_path = os.path.join(os.getcwd(), args["path"])

    if not os.path.isfile(absolute_test_path):
        print(f"ERROR: test file '{absolute_test_path}' does not exist")
        sys.exit(2)

    if pathlib.Path(absolute_test_path).suffix not in (".spec", ".sql", ".py"):
        print(
            "ERROR: Unrecognized test extension. Valid extensions are: .sql, .spec, and .py"
        )
        sys.exit(1)

    return pathlib.Path(absolute_test_path).stem


def find_test_schedule_and_line(test_name, args):
    for schedule_file_path in sorted(REGRESS_DIR.glob("*_schedule")):
        for schedule_line in open(schedule_file_path, "r"):
            if re.search(r"^test:.*\b" + test_name + r"\b", schedule_line):
                test_schedule = pathlib.Path(schedule_file_path).stem
                if args["use_whole_schedule_line"]:
                    return test_schedule, schedule_line
                return test_schedule, f"test: {test_name}\n"
    raise Exception("Test could not be found in any schedule")


def test_dependencies(test_name, test_schedule, schedule_line, args):
    if test_name in DEPS:
        return DEPS[test_name]

    if "citus_upgrade" in test_schedule:
        return TestDeps(None, citus_upgrade_infra=True)

    if schedule_line_is_upgrade_after(schedule_line):
        # upgrade_xxx_after tests always depend on upgrade_xxx_before
        test_names = schedule_line.split()[1:]
        before_tests = []
        # _after tests have implicit dependencies on _before tests
        for test_name in test_names:
            if "_after" in test_name:
                before_tests.append(test_name.replace("_after", "_before"))

        # the upgrade_columnar_before renames the schema, on which other
        # "after" tests depend. So we make sure to execute it always.
        if "upgrade_columnar_before" not in before_tests:
            before_tests.append("upgrade_columnar_before")

        return TestDeps(
            default_base_schedule(test_schedule, args),
            before_tests,
        )

    # before_ tests leave stuff around on purpose for the after tests. So they
    # are not repeatable by definition.
    if "before_" in test_schedule:
        repeatable = False
    else:
        repeatable = True

    return TestDeps(default_base_schedule(test_schedule, args), repeatable=repeatable)


# Returns true if given test_schedule_line is of the form:
#   "test: upgrade_ ... _after .."
def schedule_line_is_upgrade_after(test_schedule_line: str) -> bool:
    return (
        test_schedule_line.startswith("test: upgrade_")
        and "_after" in test_schedule_line
    )


@contextmanager
def tmp_schedule(test_name, dependencies, schedule_line, args):
    tmp_schedule_path = REGRESS_DIR / f"tmp_schedule_{random.randint(1, 10000)}"

    # Prefill the temporary schedule with the base schedule that this test
    # depends on. Some tests don't need a base schedule to run though,
    # e.g tests that are in the first place in their own schedule
    if dependencies.schedule:
        shutil.copy2(REGRESS_DIR / dependencies.schedule, tmp_schedule_path)

    with open(tmp_schedule_path, "a") as myfile:
        # Add any specific dependencies
        for dependency in dependencies.extra_tests():
            myfile.write(f"test: {dependency}\n")

        repetition_cnt = args["repeat"]
        if repetition_cnt > 1 and not dependencies.repeatable:
            repetition_cnt = 1
            print(f"WARNING: Cannot repeatably run this test: '{test_name}'")
        for _ in range(repetition_cnt):
            myfile.write(schedule_line)

    try:
        yield tmp_schedule_path.stem
    finally:
        os.remove(tmp_schedule_path)


def needed_worker_count(test_name, dependencies):
    worker_count = worker_count_for(test_name)
    for dependency in dependencies.extra_tests():
        worker_count = max(worker_count_for(dependency), worker_count)
    return worker_count


if __name__ == "__main__":
    main()
