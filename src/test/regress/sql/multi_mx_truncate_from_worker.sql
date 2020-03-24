CREATE SCHEMA truncate_from_workers;
SET search_path TO 'truncate_from_workers';

SET citus.next_shard_id TO 2380000;
SET citus.next_placement_id TO 2380000;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 6;
SET citus.replication_model TO streaming;

CREATE TABLE "refer'ence_table"(id int PRIMARY KEY);
SELECT create_reference_table('refer''ence_table');

CREATE TABLE on_update_fkey_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('on_update_fkey_table', 'id');

ALTER TABLE on_update_fkey_table ADD CONSTRAINT fkey FOREIGN KEY(value_1) REFERENCES "refer'ence_table"(id) ON UPDATE CASCADE;

INSERT INTO "refer'ence_table" SELECT i FROM generate_series(0, 100) i;
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

-- first, make sure that truncate from the coordinator workers as expected
TRUNCATE on_update_fkey_table;
SELECT count(*) FROM on_update_fkey_table;

-- fill the table again
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

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


\c - - :public_worker_1_host :worker_1_port
SET search_path TO 'truncate_from_workers';

-- make sure that TRUNCATE workes expected from the worker node
TRUNCATE on_update_fkey_table;
SELECT count(*) FROM on_update_fkey_table;

-- load some data
INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;

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
	TRUNCATE "refer'ence_table" CASCADE;
ROLLBACK;

-- test with sequential mode and CASCADE
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO sequential;
	TRUNCATE on_update_fkey_table;
	TRUNCATE "refer'ence_table" CASCADE;
ROLLBACK;

-- fill some data for the next test
\c - - :master_host :master_port
SET search_path TO 'truncate_from_workers';
INSERT INTO "refer'ence_table" SELECT i FROM generate_series(0, 100) i;

\c - - :public_worker_1_host :worker_1_port
SET search_path TO 'truncate_from_workers';

-- make sure that DMLs-SELECTs works along with TRUNCATE worker fine
BEGIN;
	INSERT INTO on_update_fkey_table SELECT i, i % 100  FROM generate_series(0, 1000) i;
	SELECT count(*) FROM on_update_fkey_table;
	TRUNCATE on_update_fkey_table;
	SELECT count(*) FROM on_update_fkey_table;
ROLLBACK;

RESET client_min_messages;

\c - - :master_host :master_port

-- also test the infrastructure that is used for supporting
-- TRUNCATE from worker nodes

-- should fail since it is not in transaction block
SELECT lock_relation_if_exists('on_update_fkey_table', 'ACCESS SHARE');

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
