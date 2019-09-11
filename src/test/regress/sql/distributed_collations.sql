SET citus.next_shard_id TO 20050000;

CREATE USER collationuser;
SELECT run_command_on_workers($$CREATE USER collationuser;$$);

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

-- This is hacky, but we should clean-up the resources as below

\c - - - :worker_1_port
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA collation_tests CASCADE;
DROP SCHEMA collation_tests2 CASCADE;

\c - - - :worker_2_port
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA collation_tests CASCADE;
DROP SCHEMA collation_tests2 CASCADE;

\c - - - :master_port

DROP USER collationuser;
SELECT run_command_on_workers($$DROP USER collationuser;$$);
