#!/usr/bin/env python3

import os
import random
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "ERROR: Expected the name of the new test as an argument, such as:\n"
            "src/test/regress/bin/create_test.py my_awesome_test"
        )
        sys.exit(1)

    test_name = sys.argv[1]

    regress_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(regress_dir, "sql", f"{test_name}.sql")

    if os.path.isfile(filename):
        print(f"ERROR: test file '{filename}' already exists")
        sys.exit(1)

    shard_id = random.randint(1, 999999) * 100

    contents = f"""CREATE SCHEMA {test_name};
SET search_path TO {test_name};
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO {shard_id};

-- add tests here

SET client_min_messages TO WARNING;
DROP SCHEMA {test_name} CASCADE;
"""

    with open(filename, "w") as f:
        f.write(contents)

    print(f"Created {filename}")
    print(f"Don't forget to add '{test_name}' in multi_schedule somewhere")
