#!/bin/sh
set -uex
# Set POSTGRES_DIR and CITUS_DIR environment variables.

pushd "$POSTGRES_DIR"
git checkout REL9_5_3
./configure --prefix=/tmp/pg --enable-cassert --enable-debug
make -sj6 install
popd


pushd "$CITUS_DIR"
PATH="/tmp/pg/bin:$PATH" ./configure
make install && make -C src/test/regress check-multi || true # because it fails
cat ./src/test/regress/tmp_check/master/log/postmaster.log
# [...]
# ERROR:  replication_factor must be positive
# STATEMENT:  SELECT master_create_worker_shards('test_schema_support.nation_hash', 4, -1);
# ERROR:  could not find any shards
# DETAIL:  No shards exist for distributed table "nation_hash".
# HINT:  Run master_create_worker_shards to create shards and try again.
# STATEMENT:  INSERT INTO test_schema_support.nation_hash(n_nationkey, n_name, n_regionkey) VALUES (6, 'FRANCE', 3);
# TRAP: BadArgument("!(shardId != 0)", File: "utils/resource_lock.c", Line: 75)
# LOG:  server process (PID 32504) was terminated by signal 6: Aborted
# DETAIL:  Failed process was running: SELECT * FROM test_schema_support.nation_hash WHERE n_nationkey = 6;
# LOG:  terminating any other active server processes
# WARNING:  terminating connection because of crash of another server process
# DETAIL:  The postmaster has commanded this server process to roll back the current transaction and exit, because another server process exited abnormally and possibly corrupted shared memory.
# HINT:  In a moment you should be able to reconnect to the database and repeat your command
# [...]
popd

