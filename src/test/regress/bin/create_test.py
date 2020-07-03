#!/usr/bin/env python3

import sys
import random

if len(sys.argv) != 2:
    raise Exception("Expected a single argument, not more not less")

test_name = sys.argv[1]

shard_id = random.randint(1, 999999) * 100

contents = f"""CREATE SCHEMA {test_name};
SET search_path TO {test_name};
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO {shard_id};

-- add tests here

\\set VERBOSITY terse
DROP SCHEMA {test_name} CASCADE;
"""


with open(f"src/test/regress/sql/{test_name}.sql", "w") as f:
    f.write(contents)


print(f"Don't forget to add \"{test_name}\" in multi_schedule somewhere")
