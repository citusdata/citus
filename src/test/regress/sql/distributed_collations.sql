SET citus.next_shard_id TO 20050000;

CREATE USER collationuser;

CREATE SCHEMA collation_tests AUTHORIZATION collationuser;
CREATE SCHEMA collation_tests2 AUTHORIZATION collationuser;

SET search_path to collation_tests;

CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

SET citus.enable_ddl_propagation TO off;

CREATE COLLATION german_phonebook_unpropagated (provider = icu, locale = 'de-u-co-phonebk');

SET citus.enable_ddl_propagation TO on;

\c - - - :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'german_phonebook%'
ORDER BY 1,2,3;
\c - - - :master_port
SET search_path to collation_tests;

CREATE TABLE test_propagate(id int, t1 text COLLATE german_phonebook,
    t2 text COLLATE german_phonebook_unpropagated);
INSERT INTO test_propagate VALUES (1, 'aesop', U&'\00E4sop'), (2, U&'Vo\1E9Er', 'Vossr');
SELECT create_distributed_table('test_propagate', 'id');

-- Test COLLATE is pushed down
SELECT * FROM collation_tests.test_propagate WHERE t2 < 'b';
SELECT * FROM collation_tests.test_propagate WHERE t2 < 'b' COLLATE "C";
SELECT * FROM collation_tests.test_propagate WHERE t2 COLLATE "C" < 'b';

-- Test COLLATE is pushed down with aggregate function
SET citus.next_shard_id TO 20060000;
CREATE TABLE test_collate_pushed_down_aggregate (a int, b int);
SELECT create_distributed_table('test_collate_pushed_down_aggregate', 'a');
SELECT alter_distributed_table('test_collate_pushed_down_aggregate', shard_count := 2, cascade_to_colocated:=false);
SET citus.log_remote_commands TO true;
SELECT ALL MIN((lower(CAST(test_collate_pushed_down_aggregate.a AS VARCHAR)) COLLATE "C"))
    FROM ONLY test_collate_pushed_down_aggregate;
RESET citus.log_remote_commands;

-- Test range table with collated distribution column
CREATE TABLE test_range(key text COLLATE german_phonebook, val int);
SELECT create_distributed_table('test_range', 'key', 'range');
SELECT master_create_empty_shard('test_range') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 'a', shardmaxvalue = 'f'
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('test_range') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 'G', shardmaxvalue = 'Z'
WHERE shardid = :new_shard_id;

-- without german_phonebook collation, this would fail
INSERT INTO test_range VALUES (U&'\00E4sop', 1), (U&'Vo\1E9Er', 2);

-- without german_phonebook collation, this would not be router executable
SET client_min_messages TO debug;
SELECT * FROM test_range WHERE key > 'Ab' AND key < U&'\00E4z';

\c - - - :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'german_phonebook%'
ORDER BY 1,2,3;
\c - - - :master_port

ALTER COLLATION collation_tests.german_phonebook RENAME TO german_phonebook2;
ALTER COLLATION collation_tests.german_phonebook2 SET SCHEMA collation_tests2;
ALTER COLLATION collation_tests2.german_phonebook2 OWNER TO collationuser;

\c - - - :worker_1_port
SELECT c.collname, nsp.nspname, a.rolname
FROM pg_collation c
JOIN pg_namespace nsp ON nsp.oid = c.collnamespace
JOIN pg_authid a ON a.oid = c.collowner
WHERE collname like 'german_phonebook%'
ORDER BY 1,2,3;
\c - - - :master_port

SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA collation_tests CASCADE;
DROP SCHEMA collation_tests2 CASCADE;
DROP USER collationuser;

\c - - - :worker_1_port
-- test creating a collation on a worker
CREATE COLLATION another_german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

-- test if creating a collation on a worker on a local
-- schema raises the right error
SET citus.enable_ddl_propagation TO off;
CREATE SCHEMA collation_creation_on_worker;
SET citus.enable_ddl_propagation TO on;

CREATE COLLATION collation_creation_on_worker.another_german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

SET citus.enable_ddl_propagation TO off;
DROP SCHEMA collation_creation_on_worker;
SET citus.enable_ddl_propagation TO on;

\c - - - :master_port

-- will skip trying to propagate the collation due to temp schema
CREATE COLLATION pg_temp.temp_collation (provider = icu, locale = 'de-u-co-phonebk');

SET client_min_messages TO ERROR;
CREATE USER alter_collation_user;
SELECT 1 FROM run_command_on_workers('CREATE USER alter_collation_user');
RESET client_min_messages;

CREATE COLLATION alter_collation FROM "C";
ALTER COLLATION alter_collation OWNER TO alter_collation_user;

SELECT result FROM run_command_on_all_nodes('
    SELECT collowner::regrole FROM pg_collation WHERE collname = ''alter_collation'';
');

DROP COLLATION alter_collation;
SET client_min_messages TO ERROR;
DROP USER alter_collation_user;
SELECT 1 FROM run_command_on_workers('DROP USER alter_collation_user');
RESET client_min_messages;
