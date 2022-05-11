CREATE SCHEMA truncate_from_workers;
SET search_path TO 'truncate_from_workers';

SET citus.next_shard_id TO 2380000;
SET citus.next_placement_id TO 2380000;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 6;

CREATE TABLE "refer'ence_table"(id int PRIMARY KEY);
SELECT create_reference_table('refer''ence_table');

CREATE TABLE on_update_fkey_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('on_update_fkey_table', 'id');

ALTER TABLE on_update_fkey_table ADD CONSTRAINT fkey FOREIGN KEY(value_1) REFERENCES "refer'ence_table"(id) ON UPDATE CASCADE;

INSERT INTO "refer'ence_table" SELECT i FROM generate_series(0, 100) i;
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- also have one replicated table
SET citus.shard_replication_factor TO 2;
CREATE TABLE replicated_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('replicated_table', 'id');
INSERT INTO replicated_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- first, make sure that truncate from the coordinator workers as expected
TRUNCATE on_update_fkey_table;
SELECT count(*) FROM on_update_fkey_table;

TRUNCATE replicated_table;
SELECT count(*) FROM replicated_table;
SET citus.task_assignment_policy TO "round-robin";
SELECT count(*) FROM replicated_table;
RESET citus.task_assignment_policy;

-- fill the tables again
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
INSERT INTO replicated_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- now, show that TRUNCATE CASCADE works expected from the coordinator
TRUNCATE "refer'ence_table" CASCADE;
SELECT count(*) FROM on_update_fkey_table;
SELECT count(*) FROM "refer'ence_table";

-- load some data for the next tests
INSERT INTO "refer'ence_table" SELECT i FROM generate_series(0, 100) i;
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- make sure that DDLs along with TRUNCATE worker fine
BEGIN;
	ALTER TABLE on_update_fkey_table ADD COLUMN x INT;
	TRUNCATE on_update_fkey_table;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;

BEGIN;
	ALTER TABLE replicated_table ADD COLUMN x INT;
	TRUNCATE replicated_table;
	SELECT count(*) FROM replicated_table;
ROLLBACK;


\c - - - :worker_1_port
SET search_path TO 'truncate_from_workers';

-- make sure that TRUNCATE workes expected from the worker node
TRUNCATE on_update_fkey_table;
SELECT count(*) FROM on_update_fkey_table;

-- make sure that TRUNCATE workes expected from the worker node
TRUNCATE replicated_table;
SELECT count(*) FROM replicated_table;

-- load some data
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
INSERT INTO replicated_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- now, show that TRUNCATE CASCADE works expected from the worker
TRUNCATE "refer'ence_table" CASCADE;
SELECT count(*) FROM on_update_fkey_table;
SELECT count(*) FROM "refer'ence_table";

-- test within transaction blocks
BEGIN;
	TRUNCATE on_update_fkey_table;
ROLLBACK;

-- test within transaction blocks
BEGIN;
	TRUNCATE replicated_table;
ROLLBACK;

-- test within transaction blocks
BEGIN;
	TRUNCATE "refer'ence_table" CASCADE;
ROLLBACK;

-- test with sequential mode and CASCADE
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO sequential;
	TRUNCATE on_update_fkey_table;
	TRUNCATE "refer'ence_table" CASCADE;
ROLLBACK;


-- test with sequential mode and CASCADE
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO sequential;
	TRUNCATE replicated_table CASCADE;
ROLLBACK;

-- fill some data for the next test
\c - - - :master_port
SET search_path TO 'truncate_from_workers';
INSERT INTO "refer'ence_table" SELECT i FROM generate_series(0, 100) i;

\c - - - :worker_1_port
SET search_path TO 'truncate_from_workers';

-- make sure that DMLs-SELECTs works along with TRUNCATE worker fine
BEGIN;
	-- we can enable local execution when truncate can be executed locally.
	SET citus.enable_local_execution = 'off';
	INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
	SELECT count(*) FROM on_update_fkey_table;
	TRUNCATE on_update_fkey_table;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;

-- make sure that DMLs-SELECTs works along with TRUNCATE worker fine
TRUNCATE replicated_table;
BEGIN;
	-- we can enable local execution when truncate can be executed locally.
	SET citus.enable_local_execution = 'off';
	INSERT INTO replicated_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
	SELECT count(*) FROM replicated_table;
	TRUNCATE replicated_table;
	SELECT count(*) FROM replicated_table;
ROLLBACK;

RESET client_min_messages;

\c - - - :master_port

-- also test the infrastructure that is used for supporting
-- TRUNCATE from worker nodes

-- should pass since we don't check for xact block in lock_relation_if_exists
SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'ACCESS SHARE');

BEGIN;
	-- should fail since the schema is not provided
	SELECT lock_relation_if_exists('on_update_fkey_table', 'ACCESS SHARE');
ROLLBACK;

BEGIN;
	-- should work since the schema is in the search path
	SET search_path TO 'truncate_from_workers';
	SELECT lock_relation_if_exists('on_update_fkey_table', 'ACCESS SHARE');
ROLLBACK;


BEGIN;
	-- should work since the schema is in the search path
	SET search_path TO 'truncate_from_workers';
	SELECT lock_relation_if_exists('replicated_table', 'ACCESS SHARE');
ROLLBACK;

BEGIN;
	-- should return false since there is no such table
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_tableXXX', 'ACCESS SHARE');
ROLLBACK;


BEGIN;
	-- should fail since there is no such lock mode
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'MY LOCK MODE');
ROLLBACK;

BEGIN;
	-- test all lock levels
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'ACCESS SHARE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'ROW SHARE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'ROW EXCLUSIVE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'SHARE UPDATE EXCLUSIVE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'SHARE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'SHARE ROW EXCLUSIVE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'SHARE ROW EXCLUSIVE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'EXCLUSIVE');
	SELECT lock_relation_if_exists('truncate_from_workers.on_update_fkey_table', 'ACCESS EXCLUSIVE');

	-- see them all
	SELECT relation::regclass, mode FROM pg_locks WHERE pid = pg_backend_pid() AND relation = 'truncate_from_workers.on_update_fkey_table'::regclass ORDER BY 2 DESC;
COMMIT;

DROP SCHEMA truncate_from_workers CASCADE;

SET search_path TO public;
