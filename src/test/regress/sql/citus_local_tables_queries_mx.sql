\set VERBOSITY terse

SET citus.next_shard_id TO 1510000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_table_queries_mx;
SET search_path TO citus_local_table_queries_mx;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- start metadata sync to worker 1
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SET citus.shard_replication_factor TO 1;

CREATE TABLE dummy_reference_table(a int unique, b int);
SELECT create_reference_table('dummy_reference_table');

CREATE TABLE citus_local_table(a int, b int);
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_to_dummy_1 FOREIGN KEY (a) REFERENCES dummy_reference_table(a);

CREATE TABLE citus_local_table_2(a int, b int);
ALTER TABLE citus_local_table_2 ADD CONSTRAINT fkey_to_dummy_2 FOREIGN KEY (a) REFERENCES dummy_reference_table(a);

CREATE TABLE reference_table(a int, b int);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table(a int, b int);
SELECT create_distributed_table('distributed_table', 'a');

\c - - - :worker_1_port
SET search_path TO citus_local_table_queries_mx;

CREATE TABLE postgres_local_table(a int, b int);

-- Define a helper function to truncate & insert some data into our test tables
-- We should call this function at some places in this test file to prevent
-- test to take a long time.
-- We shouldn't use LIMIT in INSERT SELECT queries to make the test faster as
-- LIMIT would force planner to wrap SELECT query in an intermediate result and
-- this might reduce the coverage of the test cases.
SET citus.enable_metadata_sync TO OFF;
CREATE FUNCTION clear_and_init_test_tables() RETURNS void AS $$
    BEGIN
		SET client_min_messages to ERROR;

		TRUNCATE postgres_local_table, citus_local_table, reference_table, distributed_table, dummy_reference_table, citus_local_table_2;

		INSERT INTO dummy_reference_table SELECT i, i FROM generate_series(0, 5) i;
		INSERT INTO citus_local_table SELECT i, i FROM generate_series(0, 5) i;
		INSERT INTO citus_local_table_2 SELECT i, i FROM generate_series(0, 5) i;
		INSERT INTO postgres_local_table SELECT i, i FROM generate_series(0, 5) i;
		INSERT INTO distributed_table SELECT i, i FROM generate_series(0, 5) i;
		INSERT INTO reference_table SELECT i, i FROM generate_series(0, 5) i;

		RESET client_min_messages;
    END;
$$ LANGUAGE plpgsql;
RESET citus.enable_metadata_sync;

----------------
---- SELECT ----
----------------

SELECT clear_and_init_test_tables();

-- join between citus local tables and reference tables would succeed
SELECT count(*) FROM citus_local_table, reference_table WHERE citus_local_table.a = reference_table.a;
SELECT * FROM citus_local_table, reference_table WHERE citus_local_table.a = reference_table.a ORDER BY 1,2,3,4 FOR UPDATE;

-- should work
WITH cte_1 AS
	(SELECT * FROM citus_local_table, reference_table WHERE citus_local_table.a = reference_table.a ORDER BY 1,2,3,4 FOR UPDATE)
SELECT count(*) FROM cte_1;

-- should work as joins are between ctes
WITH cte_citus_local_table AS
	(SELECT * FROM citus_local_table),
cte_postgres_local_table AS
	(SELECT * FROM postgres_local_table),
cte_distributed_table AS
	(SELECT * FROM distributed_table)
SELECT count(*) FROM cte_distributed_table, cte_citus_local_table, cte_postgres_local_table
WHERE  cte_citus_local_table.a = 1 AND cte_distributed_table.a = 1;

-- should fail as we don't support direct joins between distributed/local tables
SELECT count(*) FROM distributed_table d1, distributed_table d2, citus_local_table;

-- local table inside subquery should just work
SELECT count(*) FROM
(
	SELECT * FROM (SELECT * FROM citus_local_table) as subquery_inner
) as subquery_top;

SELECT clear_and_init_test_tables();

-- join between citus/postgres local tables wouldn't work as citus local table is on the coordinator
SELECT count(*) FROM
(
	SELECT * FROM (SELECT count(*) FROM citus_local_table, postgres_local_table) as subquery_inner
) as subquery_top;

-- should fail as we don't support direct joins between distributed/local tables
SELECT count(*) FROM
(
	SELECT *, random() FROM (SELECT *, random() FROM citus_local_table, distributed_table) as subquery_inner
) as subquery_top;

-- should fail as we don't support direct joins between distributed/local tables
SELECT count(*) FROM
(
	SELECT *, random()
		FROM (
				WITH cte_1 AS (SELECT *, random() FROM citus_local_table, distributed_table) SELECT * FROM cte_1) as subquery_inner
) as subquery_top;

-- should be  fine
SELECT count(*) FROM
(
	SELECT *, random()
		FROM (
				WITH cte_1 AS (SELECT *, random() FROM citus_local_table), cte_2  AS (SELECT * FROM distributed_table) SELECT count(*) FROM cte_1, cte_2
				) as subquery_inner
) as subquery_top;

SELECT clear_and_init_test_tables();

-- prepared statement
PREPARE citus_local_only AS SELECT count(*) FROM citus_local_table;

-- execute 6 times, local tables without params
EXECUTE citus_local_only;
EXECUTE citus_local_only;
EXECUTE citus_local_only;
EXECUTE citus_local_only;
EXECUTE citus_local_only;
EXECUTE citus_local_only;

-- execute 6 times, with param
PREPARE citus_local_only_p(int) AS SELECT count(*) FROM citus_local_table WHERE a = $1;
EXECUTE citus_local_only_p(1);
EXECUTE citus_local_only_p(1);
EXECUTE citus_local_only_p(1);
EXECUTE citus_local_only_p(1);
EXECUTE citus_local_only_p(1);
EXECUTE citus_local_only_p(1);

-- do not evalute the function
-- show the logs
EXECUTE citus_local_only_p(random());
EXECUTE citus_local_only_p(random());

PREPARE mixed_query(int,  int, int) AS
	WITH cte_citus_local_table AS
		(SELECT * FROM citus_local_table WHERE a = $1),
	cte_postgres_local_table AS
		(SELECT * FROM postgres_local_table WHERE a = $2),
	cte_distributed_table AS
		(SELECT * FROM distributed_table WHERE a = $3),
	cte_mixes AS (SELECT * FROM cte_distributed_table, cte_citus_local_table, cte_postgres_local_table)
	SELECT count(*) FROM cte_mixes;

EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);
EXECUTE mixed_query(1,2,3);

SELECT clear_and_init_test_tables();

-- anonymous columns
WITH a AS (SELECT a, '' FROM citus_local_table GROUP BY a) SELECT a.a FROM a ORDER BY 1 LIMIT 5;
WITH a AS (SELECT b, '' FROM citus_local_table WHERE a = 1) SELECT * FROM a, a b ORDER BY 1 LIMIT 5;

-- set operations should just work
SELECT * FROM citus_local_table UNION SELECT * FROM postgres_local_table UNION SELECT * FROM distributed_table ORDER BY 1,2;
(SELECT * FROM citus_local_table ORDER BY 1,2 LIMIT 5) INTERSECT (SELECT i, i FROM generate_series(0, 100) i) ORDER BY 1, 2;

-- should just work as recursive planner kicks in
SELECT count(*) FROM distributed_table WHERE a IN (SELECT a FROM citus_local_table);
SELECT count(*) FROM citus_local_table  WHERE a IN (SELECT a FROM distributed_table);

SELECT count(*) FROM reference_table WHERE a IN (SELECT a FROM citus_local_table);
SELECT count(*) FROM citus_local_table  WHERE a IN (SELECT a FROM reference_table);


--  nested recursive queries should just work
SELECT count(*)  FROM citus_local_table
	WHERE a IN
	(SELECT a FROM distributed_table WHERE a IN
	 (SELECT b FROM citus_local_table WHERE b IN (SELECT b FROM postgres_local_table)));

-- local outer joins
SELECT count(*) FROM citus_local_table LEFT JOIN reference_table ON (true);
SELECT count(*) FROM reference_table
  LEFT JOIN citus_local_table ON (true)
  LEFT JOIN postgres_local_table ON (true)
  LEFT JOIN reference_table r2 ON (true);

-- supported outer join
SELECT count(*) FROM citus_local_table LEFT JOIN distributed_table ON (true);

-- distinct in subquery on CTE
WITH one_row AS (
	SELECT a from citus_local_table WHERE b = 1
)
SELECT
  *
FROM
  distributed_table
WHERE
  b IN (SELECT DISTINCT a FROM one_row)
ORDER BY
  1, 2
LIMIT
  1;

WITH one_row_2 AS (
	SELECT a from distributed_table WHERE b = 1
)
SELECT
  *
FROM
  citus_local_table
WHERE
  b IN (SELECT DISTINCT a FROM one_row_2)
ORDER BY
  1 ,2
LIMIT
  1;

-- join between citus local tables and distributed tables would fail
SELECT count(*) FROM citus_local_table, distributed_table;
SELECT * FROM citus_local_table, distributed_table ORDER BY 1,2,3,4 FOR UPDATE;

-- join between citus local table and postgres local table would fail
-- as citus local table is on the coordinator
SELECT count(citus_local_table.b), count(postgres_local_table.a)
FROM citus_local_table, postgres_local_table
WHERE citus_local_table.a = postgres_local_table.b;

-- select for update is just OK
SELECT * FROM citus_local_table ORDER BY 1,2 FOR UPDATE;

---------------------------
----- INSERT SELECT -----
---------------------------

-- simple INSERT SELECT is OK

SELECT clear_and_init_test_tables();

INSERT INTO citus_local_table
SELECT * from reference_table;

INSERT INTO reference_table
SELECT * from citus_local_table;

INSERT INTO citus_local_table
SELECT * from distributed_table;

INSERT INTO distributed_table
SELECT * from citus_local_table;

INSERT INTO citus_local_table
SELECT * from citus_local_table_2;

INSERT INTO citus_local_table
SELECT * from citus_local_table_2
ORDER BY 1,2
LIMIT 10;

INSERT INTO citus_local_table
SELECT * from postgres_local_table;

INSERT INTO postgres_local_table
SELECT * from citus_local_table;

-- INSERT SELECT with local joins are OK

SELECT clear_and_init_test_tables();

INSERT INTO citus_local_table
SELECT reference_table.* FROM reference_table
JOIN citus_local_table ON (true);

INSERT INTO reference_table
SELECT reference_table.* FROM reference_table
JOIN citus_local_table ON (true);

INSERT INTO reference_table
SELECT reference_table.* FROM reference_table, postgres_local_table
JOIN citus_local_table ON (true);

SELECT clear_and_init_test_tables();

INSERT INTO distributed_table
SELECT reference_table.* FROM reference_table
JOIN citus_local_table ON (true);

INSERT INTO distributed_table
SELECT reference_table.* FROM reference_table, postgres_local_table
JOIN citus_local_table ON (true);

INSERT INTO postgres_local_table
SELECT reference_table.* FROM reference_table
JOIN citus_local_table ON (true);

-- INSERT SELECT that joins reference and distributed tables is also OK

SELECT clear_and_init_test_tables();

INSERT INTO citus_local_table
SELECT reference_table.* FROM reference_table
JOIN distributed_table ON (true);

INSERT INTO citus_local_table
SELECT reference_table.*
FROM reference_table, distributed_table;

-- INSERT SELECT that joins citus local and distributed table directly will fail ..
INSERT INTO citus_local_table
SELECT distributed_table.* FROM distributed_table
JOIN citus_local_table ON (true);

-- .. but when wrapped into a CTE, join works fine
INSERT INTO citus_local_table
SELECT distributed_table.* FROM distributed_table
JOIN (WITH cte AS (SELECT * FROM citus_local_table) SELECT * FROM cte) as foo ON (true);

-- multi row insert is OK
INSERT INTO citus_local_table VALUES (1, 2), (3, 4);

---------------------------
----- DELETE / UPDATE -----
---------------------------

-- modifications using citus local tables and postgres local tables
-- are not supported, see below four tests

SELECT clear_and_init_test_tables();

DELETE FROM citus_local_table
USING postgres_local_table
WHERE citus_local_table.b = postgres_local_table.b;

UPDATE citus_local_table
SET b = 5
FROM postgres_local_table
WHERE citus_local_table.a = 3 AND citus_local_table.b = postgres_local_table.b;

DELETE FROM postgres_local_table
USING citus_local_table
WHERE citus_local_table.b = postgres_local_table.b;

UPDATE postgres_local_table
SET b = 5
FROM citus_local_table
WHERE citus_local_table.a = 3 AND citus_local_table.b = postgres_local_table.b;

-- no direct joins supported
UPDATE distributed_table
SET b = 6
FROM citus_local_table
WHERE citus_local_table.a = distributed_table.a;

UPDATE reference_table
SET b = 6
FROM citus_local_table
WHERE citus_local_table.a = reference_table.a;

-- should not work, add HINT use CTEs
UPDATE citus_local_table
SET b = 6
FROM distributed_table
WHERE citus_local_table.a = distributed_table.a;

-- should work, add HINT use CTEs
UPDATE citus_local_table
SET b = 6
FROM reference_table
WHERE citus_local_table.a = reference_table.a;

-- should not work, add HINT use CTEs
DELETE FROM distributed_table
USING citus_local_table
WHERE citus_local_table.a = distributed_table.a;

-- should not work, add HINT use CTEs
DELETE FROM citus_local_table
USING distributed_table
WHERE citus_local_table.a = distributed_table.a;

DELETE FROM reference_table
USING citus_local_table
WHERE citus_local_table.a = reference_table.a;

-- should work, add HINT use CTEs
DELETE FROM citus_local_table
USING reference_table
WHERE citus_local_table.a = reference_table.a;

-- just works
DELETE FROM citus_local_table
WHERE citus_local_table.a IN (SELECT a FROM distributed_table);

-- just works
DELETE FROM citus_local_table
WHERE citus_local_table.a IN (SELECT a FROM reference_table);

-- just works
WITH distributed_table_cte AS (SELECT * FROM distributed_table)
UPDATE citus_local_table
SET b = 6
FROM distributed_table_cte
WHERE citus_local_table.a = distributed_table_cte.a;

-- just works
WITH reference_table_cte AS (SELECT * FROM reference_table)
UPDATE citus_local_table
SET b = 6
FROM reference_table_cte
WHERE citus_local_table.a = reference_table_cte.a;

------------------------
----- VIEW QUERIES -----
------------------------

CREATE MATERIALIZED VIEW mat_view_4 AS
SELECT count(*)
FROM citus_local_table
JOIN reference_table
USING (a);

-- ok
SELECT count(*) FROM mat_view_4;

-- should work
SELECT count(*) FROM distributed_table WHERE b in
(SELECT count FROM mat_view_4);

SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW view_2 AS
SELECT count(*)
FROM citus_local_table
JOIN citus_local_table_2 USING (a)
JOIN distributed_table USING (a);
RESET citus.enable_ddl_propagation;

-- should fail as view contains direct local dist join
SELECT count(*) FROM view_2;

SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW view_3
AS SELECT count(*)
FROM citus_local_table_2
JOIN reference_table
USING (a);
RESET citus.enable_ddl_propagation;

-- ok
SELECT count(*) FROM view_3;

-- view treated as subquery, so should work
SELECT count(*) FROM view_3, distributed_table;

----------------------------------------------
-- Some other tests with subqueries & CTE's --
----------------------------------------------

SELECT clear_and_init_test_tables();

SELECT count(*) AS a, count(*) AS b
FROM reference_table
JOIN (SELECT count(*) as a, count(*) as b
      FROM citus_local_table_2
      JOIN (SELECT count(*) as a, count(*) as b
            FROM postgres_local_table
            JOIN (SELECT count(*) as a, count(*) as b
                  FROM reference_table as table_4677) subquery5108
            USING (a)) subquery7132
      USING (b)) subquery7294
USING (a);

-- direct join inside CTE not supported
WITH cte AS (
UPDATE citus_local_table lt SET a = mt.a
FROM distributed_table mt WHERE mt.b = lt.b
RETURNING lt.b, lt.a
) SELECT * FROM cte JOIN distributed_table mt ON mt.b = cte.b ORDER BY 1,2,3,4;

-- join with CTE just works
UPDATE citus_local_table
SET a=5
FROM (SELECT avg(distributed_table.b) as avg_b
      FROM distributed_table) as foo
WHERE
foo.avg_b = citus_local_table.b;

-- should work
UPDATE distributed_table
SET b = avg_a
FROM (SELECT avg(citus_local_table.a) as avg_a FROM citus_local_table) as foo
WHERE foo.avg_a = distributed_table.a
RETURNING distributed_table.*;

-- it is unfortunate that recursive planner cannot detect this
-- but expected to not work
UPDATE citus_local_table
SET a=5
FROM (SELECT b FROM distributed_table) AS foo
WHERE foo.b = citus_local_table.b;

------------------------------------
-- test different execution paths --
------------------------------------

-- a bit different explain output than for postgres local tables
EXPLAIN (COSTS FALSE)
INSERT INTO citus_local_table
SELECT * FROM distributed_table
ORDER BY distributed_table.*
LIMIT 10;

-- show that we do not pull to coordinator
EXPLAIN (COSTS FALSE)
INSERT INTO citus_local_table
SELECT * FROM citus_local_table;

EXPLAIN (COSTS FALSE)
INSERT INTO citus_local_table
SELECT reference_table.* FROM reference_table;

EXPLAIN (COSTS FALSE)
INSERT INTO citus_local_table
SELECT reference_table.* FROM reference_table, postgres_local_table;

-- show that we pull to coordinator when a distributed table is involved
EXPLAIN (COSTS FALSE)
INSERT INTO citus_local_table
SELECT reference_table.* FROM reference_table, distributed_table;

-- truncate tables & add unique constraints to be able to define foreign keys
TRUNCATE reference_table, citus_local_table, distributed_table;

\c - - - :master_port
SET search_path TO citus_local_table_queries_mx;
SET citus.shard_replication_factor TO 1;

ALTER TABLE reference_table ADD CONSTRAINT pkey_ref PRIMARY KEY (a);
ALTER TABLE citus_local_table ADD CONSTRAINT pkey_c PRIMARY KEY (a);

-- define a foreign key chain distributed table -> reference table -> citus local table
-- to test sequential execution
ALTER TABLE distributed_table ADD CONSTRAINT fkey_dist_to_ref FOREIGN KEY(a) REFERENCES reference_table(a) ON DELETE RESTRICT;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(a) REFERENCES citus_local_table(a) ON DELETE RESTRICT;

\c - - - :worker_1_port
SET search_path TO citus_local_table_queries_mx;

INSERT INTO citus_local_table VALUES (1);
INSERT INTO reference_table VALUES (1);

BEGIN;
    INSERT INTO citus_local_table VALUES (1) ON CONFLICT (a) DO NOTHING;
    INSERT INTO distributed_table VALUES (1);

    -- should show sequential as first inserting into citus local table
	-- would force the xact block to use sequential execution
    show citus.multi_shard_modify_mode;
ROLLBACK;

BEGIN;
	TRUNCATE distributed_table;

	-- should error out as we truncated distributed_table via parallel execution
	TRUNCATE citus_local_table  CASCADE;
ROLLBACK;

BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	TRUNCATE distributed_table;

	-- should work fine as we already switched to sequential execution
	-- before parallel truncate
	TRUNCATE citus_local_table  CASCADE;
ROLLBACK;

\c - - - :master_port
SET search_path TO citus_local_table_queries_mx;
SET citus.shard_replication_factor TO 1;

ALTER TABLE distributed_table DROP CONSTRAINT fkey_dist_to_ref;

\c - - - :worker_1_port
SET search_path TO citus_local_table_queries_mx;

BEGIN;
    INSERT INTO citus_local_table VALUES (1) ON CONFLICT (a) DO NOTHING;
    show citus.multi_shard_modify_mode;
ROLLBACK;

\c - - - :master_port
SET search_path TO citus_local_table_queries_mx;
SET citus.shard_replication_factor TO 1;

-- remove uniqueness constraint and dependent foreign key constraint for next tests
ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;
ALTER TABLE citus_local_table DROP CONSTRAINT pkey_c;

\c - - - :worker_1_port
SET search_path TO citus_local_table_queries_mx;

COPY  citus_local_table(a) FROM  PROGRAM 'seq 1';

BEGIN;
	COPY  citus_local_table(a) FROM  PROGRAM 'seq 1';
	COPY  citus_local_table(a) FROM  PROGRAM 'seq 1';
COMMIT;

COPY citus_local_table TO STDOUT;
COPY (SELECT * FROM citus_local_table) TO STDOUT;

BEGIN;
  COPY citus_local_table TO STDOUT;
COMMIT;

BEGIN;
  COPY (SELECT * FROM citus_local_table) TO STDOUT;
COMMIT;

-- truncate test tables for next test
TRUNCATE citus_local_table, reference_table, distributed_table;

BEGIN;
	INSERT INTO citus_local_table VALUES (1), (2);

	SAVEPOINT sp1;
	INSERT INTO citus_local_table VALUES (3), (4);

	ROLLBACK TO SAVEPOINT sp1;
	SELECT * FROM citus_local_table ORDER BY 1,2;

	SAVEPOINT sp2;
	INSERT INTO citus_local_table VALUES (3), (4);
	INSERT INTO distributed_table VALUES (3), (4);

	ROLLBACK TO SAVEPOINT sp2;
	SELECT * FROM citus_local_table ORDER BY 1,2;
	SELECT * FROM distributed_table ORDER BY 1,2;

	SAVEPOINT sp3;
	INSERT INTO citus_local_table VALUES (3), (2);
	INSERT INTO reference_table VALUES (3), (2);

	ROLLBACK TO SAVEPOINT sp3;
	SELECT * FROM citus_local_table ORDER BY 1,2;
	SELECT * FROM reference_table ORDER BY 1,2;
COMMIT;

\c - - - :master_port
-- cleanup at exit
DROP SCHEMA citus_local_table_queries_mx CASCADE;
