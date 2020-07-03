#!/usr/bin/env python3

import sys
import random
import os

if len(sys.argv) != 2:
    raise Exception("Expected the name of the new test as an argument")

test_name = sys.argv[1]

filename = f"src/test/regress/sql/{test_name}.sql"

if os.path.isfile(filename):
    raise Exception(f"test file '{filename}' already exists")

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


print(f"Don't forget to add '{test_name}' in multi_schedule somewhere")
