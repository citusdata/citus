#!/usr/bin/env python3

import argparse
import os
import pathlib
import random
import re
import shutil
import sys
from collections import OrderedDict
from glob import glob

import common

import config

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
args.add_argument("-r", "--repeat", help="Number of test to run", type=int, default=1)
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

args = vars(args.parse_args())

regress_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
test_file_path = args["path"]
test_file_name = args["test_name"]
use_base_schedule = args["use_base_schedule"]
use_whole_schedule_line = args["use_whole_schedule_line"]


class TestDeps:
    schedule: str | None
    direct_extra_tests: list[str]

    def __init__(self, schedule, extra_tests=None, repeatable=True):
        self.schedule = schedule
        self.direct_extra_tests = extra_tests or []
        self.repeatable = repeatable

    def extra_tests(self):
        all_deps = OrderedDict()
        for direct_dep in self.direct_extra_tests:
            if direct_dep in deps:
                for indirect_dep in deps[direct_dep].extra_tests():
                    all_deps[indirect_dep] = True
            all_deps[direct_dep] = True

        return list(all_deps.keys())


deps = {
    "multi_cluster_management": TestDeps(
        None, ["multi_test_helpers_superuser"], repeatable=False
    ),
    "create_role_propagation": TestDeps(None, ["multi_cluster_management"]),
    "single_node_enterprise": TestDeps(None),
    "multi_extension": TestDeps(None, repeatable=False),
    "multi_test_helpers": TestDeps(None),
    "multi_insert_select": TestDeps("base_schedule"),
}

if not (test_file_name or test_file_path):
    print("FATAL: No test given.")
    sys.exit(2)


if test_file_path:
    test_file_path = os.path.join(os.getcwd(), args["path"])

    if not os.path.isfile(test_file_path):
        print(f"ERROR: test file '{test_file_path}' does not exist")
        sys.exit(2)

    test_file_extension = pathlib.Path(test_file_path).suffix
    test_file_name = pathlib.Path(test_file_path).stem

    if test_file_extension not in ".spec.sql":
        print(
            "ERROR: Unrecognized test extension. Valid extensions are: .sql and .spec"
        )
        sys.exit(1)

test_schedule = ""
dependencies = []

# find related schedule
for schedule_file_path in sorted(glob(os.path.join(regress_dir, "*_schedule"))):
    for schedule_line in open(schedule_file_path, "r"):
        if re.search(r"\b" + test_file_name + r"\b", schedule_line):
            test_schedule = pathlib.Path(schedule_file_path).stem
            if use_whole_schedule_line:
                test_schedule_line = schedule_line
            else:
                test_schedule_line = f"test: {test_file_name}\n"
            break
    else:
        continue
    break
else:
    raise Exception("Test could not be found in any schedule")


def default_base_schedule(test_schedule):
    if "isolation" in test_schedule:
        return "base_isolation_schedule"

    if "failure" in test_schedule:
        return "failure_base_schedule"

    if "enterprise" in test_schedule:
        return "enterprise_minimal_schedule"

    if "split" in test_schedule:
        return "minimal_schedule"

    if "mx" in test_schedule:
        if use_base_schedule:
            return "mx_base_schedule"
        return "mx_minimal_schedule"

    if "operations" in test_schedule:
        return "minimal_schedule"

    if test_schedule in config.ARBITRARY_SCHEDULE_NAMES:
        print(f"WARNING: Arbitrary config schedule ({test_schedule}) is not supported.")
        sys.exit(0)

    if use_base_schedule:
        return "base_schedule"
    return "minimal_schedule"


if test_file_name in deps:
    dependencies = deps[test_file_name]
else:
    dependencies = TestDeps(default_base_schedule(test_schedule))

# copy base schedule to a temp file and append test_schedule_line
# to be able to run tests in parallel (if test_schedule_line is a parallel group.)
tmp_schedule_path = os.path.join(
    regress_dir, f"tmp_schedule_{ random.randint(1, 10000)}"
)
# some tests don't need a schedule to run
# e.g tests that are in the first place in their own schedule
if dependencies.schedule:
    shutil.copy2(os.path.join(regress_dir, dependencies.schedule), tmp_schedule_path)
with open(tmp_schedule_path, "a") as myfile:
    for dependency in dependencies.extra_tests():
        myfile.write(f"test: {dependency}\n")

    for i in range(args["repeat"]):
        if not dependencies.repeatable and i > 0:
            print(f"WARNING: Cannot repeatably run this test: '{test_file_name}'")
            break
        myfile.write(test_schedule_line)


# find suitable make recipe
if dependencies.schedule == "base_isolation_schedule":
    make_recipe = "check-isolation-custom-schedule"
elif dependencies.schedule == "failure_base_schedule":
    make_recipe = "check-failure-custom-schedule"
else:
    make_recipe = "check-custom-schedule"

if args["valgrind"]:
    make_recipe += "-vg"

# prepare command to run tests
test_command = f"make -C {regress_dir} {make_recipe} SCHEDULE='{pathlib.Path(tmp_schedule_path).stem}'"

# run test command n times
try:
    print(f"Executing.. {test_command}")
    result = common.run(test_command)
finally:
    # remove temp schedule file
    os.remove(tmp_schedule_path)
