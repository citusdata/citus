---
--- tests around access tracking within transaction blocks
---


CREATE SCHEMA access_tracking;

SET search_path TO 'access_tracking';

CREATE FUNCTION relation_accessed_in_transaction_block(relationId Oid)
    RETURNS boolean
    LANGUAGE C STABLE STRICT
    AS 'citus', $$relation_accessed_in_transaction_block$$;


-- the tests could actually be applied to non-distributed tables as well
-- however we still prefer to test with distributed tables
-- but to make the tests run faster, we decrease the shard count
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
CREATE TABLE table_1 (a int);
SELECT create_distributed_table('table_1', 'a');
CREATE TABLE table_2 (a int);
SELECT create_distributed_table('table_2', 'a');
CREATE TABLE table_3 (a int);
SELECT create_distributed_table('table_3', 'a');
CREATE TABLE table_4 (a int);
SELECT create_distributed_table('table_4', 'a');
CREATE TABLE table_5 (a int);
SELECT create_distributed_table('table_5', 'a');


-- outisde the transaction blocks, the function always return false
SELECT count(*) FROM table_1;
SELECT relation_accessed_in_transaction_block('table_1'::regclass);

-- a very simple test that check the fast path locks
BEGIN;

	SELECT relation_accessed_in_transaction_block('table_2'::regclass);

	SELECT count(*) FROM table_1;

	SELECT relation_accessed_in_transaction_block('table_2'::regclass);
	SELECT relation_accessed_in_transaction_block('table_1'::regclass);
COMMIT;


-- again fast path locks, with a modify query
BEGIN;

	SELECT relation_accessed_in_transaction_block('table_1'::regclass);

	INSERT INTO table_1 VALUES (1);
	SELECT relation_accessed_in_transaction_block('table_1'::regclass);

	-- after one more modification, we should still see that
	DELETE FROM table_1;
	SELECT relation_accessed_in_transaction_block('table_1'::regclass);

	-- now, lets increment the lock level
	TRUNCATE table_1;
	SELECT relation_accessed_in_transaction_block('table_1'::regclass);

COMMIT;

BEGIN;
	-- ALTER TABLE should acquire a relation lock
	ALTER TABLE table_1 ADD COLUMN b INT;
	SELECT relation_accessed_in_transaction_block('table_1'::regclass);
ROLLBACK;

BEGIN;

	-- a join touches multiple tables
	SELECT 
		count(*) 
	FROM 
		table_1, table_2, table_3, table_4
	WHERE
		table_1.a = table_2.a AND table_2.a = table_3.a AND 
		table_3.a = table_4.a;

	SELECT relation_accessed_in_transaction_block('table_1'::regclass);
	SELECT relation_accessed_in_transaction_block('table_2'::regclass);
	SELECT relation_accessed_in_transaction_block('table_3'::regclass);
	SELECT relation_accessed_in_transaction_block('table_4'::regclass);

	-- we haven't accessed this table
	SELECT relation_accessed_in_transaction_block('table_5'::regclass);
ROLLBACK;

-- this test is slightly different that the others
-- here we're adding constraints to make sure that
-- the fast-path slots becomes full and we can still
-- see all the locks
BEGIN;
        -- a join touches multiple tables
        SELECT 
                count(*) 
        FROM 
                table_1, table_2, table_3, table_4
        WHERE
                table_1.a = table_2.a AND table_2.a = table_3.a AND 
                table_3.a = table_4.a;

        ALTER TABLE table_1 ADD CONSTRAINT table_1_u UNIQUE (a);
        ALTER TABLE table_2 ADD CONSTRAINT table_2_u FOREIGN KEY (a) REFERENCES table_1(a);
		ALTER TABLE table_3 ADD CONSTRAINT table_3_u FOREIGN KEY (a) REFERENCES table_1(a);
		ALTER TABLE table_4 ADD CONSTRAINT table_4_u FOREIGN KEY (a) REFERENCES table_1(a);

		-- pg_dist_node is in the fast path and can be seen via relation_accessed_in_transaction_block
        SELECT fastpath FROM pg_locks WHERE pid = pg_backend_pid() AND relation = 'pg_dist_node'::regclass;
        SELECT relation_accessed_in_transaction_block('pg_dist_node'::regclass);

        -- even if table 2 is not in the fast path, we can still see it via relation_accessed_in_transaction_block
        SELECT fastpath FROM pg_locks WHERE pid = pg_backend_pid() AND relation = 'table_2'::regclass;
        SELECT relation_accessed_in_transaction_block('table_2'::regclass);

ROLLBACK;

SET search_path TO 'public';
DROP SCHEMA access_tracking CASCADE;
