#!/usr/bin/env python3

import sys
import os
import pathlib
from glob import glob
import argparse
import shutil
import random
import re
import common
import config

args = argparse.ArgumentParser()
args.add_argument("-t", "--test", required=True, help="Relative path for test file (must have a .sql or .spec extension)", type=pathlib.Path)
args.add_argument("-n", "--ntimes", required=True, help="Number of test to run", type=int)
args.add_argument("-s", "--schedule", required=False, help="Test schedule to be used as a base (optional)", nargs='?', const='', default='')

args = vars(args.parse_args())

regress_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
test_file_path = os.path.join(os.getcwd(), args['test'])

if not os.path.isfile(test_file_path):
    print(f"ERROR: test file '{test_file_path}' does not exist")
    sys.exit(2)

test_file_extension = pathlib.Path(test_file_path).suffix
test_file_name = pathlib.Path(test_file_path).stem

if not test_file_extension in '.spec.sql':
    print(
        "ERROR: Unrecognized test extension. Valid extensions are: .sql and .spec"
    )
    sys.exit(1)

test_schedule = ''

# find related schedule
for schedule_file_path in sorted(glob(os.path.join(regress_dir, "*_schedule"))):
        for schedule_line in open(schedule_file_path, 'r'):
            if  re.search(r'\b' + test_file_name + r'\b', schedule_line):
                test_schedule = pathlib.Path(schedule_file_path).stem
                test_schedule_line = schedule_line
                break
        else:
            continue
        break

# map suitable schedule
if not test_schedule:
    print(
        f"WARNING: Could not find any schedule for '{test_file_name}'"
    )
    sys.exit(0)
elif "isolation" in test_schedule:
    test_schedule = 'base_isolation_schedule'
elif "failure" in test_schedule:
    test_schedule = 'failure_base_schedule'
elif "mx" in test_schedule:
    test_schedule = 'mx_base_schedule'
elif test_schedule in config.ARBITRARY_SCHEDULE_NAMES:
    print(f"WARNING: Arbitrary config schedule ({test_schedule}) is not supported.")
    sys.exit(0)
else:
    test_schedule = 'base_schedule'

# override if -s/--schedule is passed
if args['schedule']:
    test_schedule = args['schedule']

# copy base schedule to a temp file and append test_schedule_line
# to be able to run tests in parallel (if test_schedule_line is a parallel group.)
tmp_schedule_path = os.path.join(regress_dir, f"tmp_schedule_{ random.randint(1, 10000)}")
shutil.copy2(os.path.join(regress_dir, test_schedule), tmp_schedule_path)
with open(tmp_schedule_path, "a") as myfile:
        for i in range(args['ntimes']):
            myfile.write(test_schedule_line)

# find suitable make recipe
if "isolation" in test_schedule:
    make_recipe = 'check-isolation-custom-schedule'
else:
    make_recipe = 'check-custom-schedule'

# prepare command to run tests
test_command = f"make -C {regress_dir} {make_recipe} SCHEDULE='{pathlib.Path(tmp_schedule_path).stem}'"

# run test command n times
try:
    print(f"Executing.. {test_command}")
    result = common.run(test_command)
finally:
    # remove temp schedule file
    os.remove(tmp_schedule_path)
