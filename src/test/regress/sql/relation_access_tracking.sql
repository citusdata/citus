
---
--- tests around access tracking within transaction blocks
---

CREATE SCHEMA access_tracking;
SET search_path TO 'access_tracking';

CREATE OR REPLACE FUNCTION relation_select_access_mode(relationId Oid)
    RETURNS int
    LANGUAGE C STABLE STRICT
    AS 'citus', $$relation_select_access_mode$$;

CREATE OR REPLACE FUNCTION relation_dml_access_mode(relationId Oid)
    RETURNS int
    LANGUAGE C STABLE STRICT
    AS 'citus', $$relation_dml_access_mode$$;

CREATE OR REPLACE FUNCTION relation_ddl_access_mode(relationId Oid)
    RETURNS int
    LANGUAGE C STABLE STRICT
    AS 'citus', $$relation_ddl_access_mode$$;

CREATE OR REPLACE FUNCTION distributed_relation(relation_name text)
RETURNS bool AS
$$
DECLARE
	part_method char;
BEGIN
		 select partmethod INTO part_method from pg_dist_partition  WHERE logicalrelid = relation_name::regclass;
         IF part_method = 'h' THEN
                RETURN true;
         ELSE
                RETURN false;
         END IF;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


CREATE OR REPLACE FUNCTION relation_access_mode_to_text(relation_name text, relationShardAccess int)
RETURNS text AS
$$
BEGIN
	 IF relationShardAccess = 0 and distributed_relation(relation_name) THEN
	 	RETURN 'not_parallel_accessed';
	 ELSIF relationShardAccess = 0 and NOT distributed_relation(relation_name) THEN
	 	RETURN 'not_accessed';
	 ELSIF relationShardAccess = 1 THEN
	 	RETURN 'reference_table_access';
	 ELSE
		RETURN 'parallel_access';
	 END IF;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;



CREATE VIEW relation_accesses AS
	SELECT table_name,
			relation_access_mode_to_text(table_name, relation_select_access_mode(table_name::regclass)) as select_access,
			relation_access_mode_to_text(table_name, relation_dml_access_mode(table_name::regclass)) as dml_access,
			relation_access_mode_to_text(table_name, relation_ddl_access_mode(table_name::regclass)) as ddl_access
	FROM
		((SELECT 'table_' || i as table_name FROM generate_series(1, 7) i) UNION (SELECT 'partitioning_test') UNION (SELECT 'partitioning_test_2009') UNION (SELECT 'partitioning_test_2010')) tables;

SET citus.shard_replication_factor TO 1;
CREATE TABLE table_1 (key int, value int);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value int);
SELECT create_distributed_table('table_2', 'key');

CREATE TABLE table_3 (key int, value int);
SELECT create_distributed_table('table_3', 'key');

CREATE TABLE table_4 (key int, value int);
SELECT create_distributed_table('table_4', 'key');

CREATE TABLE table_5 (key int, value int);
SELECT create_distributed_table('table_5', 'key');

CREATE TABLE table_6 (key int, value int);
SELECT create_reference_Table('table_6');

INSERT INTO table_1 SELECT i, i FROM generate_series(0,100) i;
INSERT INTO table_2 SELECT i, i FROM generate_series(0,100) i;
INSERT INTO table_3 SELECT i, i FROM generate_series(0,100) i;
INSERT INTO table_4 SELECT i, i FROM generate_series(0,100) i;
INSERT INTO table_5 SELECT i, i FROM generate_series(0,100) i;
INSERT INTO table_6 SELECT i, i FROM generate_series(0,100) i;

-- create_distributed_table works fine
BEGIN;
	CREATE TABLE table_7 (key int, value int);
	SELECT create_distributed_table('table_7', 'key');
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_7')  ORDER BY 1;
COMMIT;

-- outside the transaction blocks, the function always returns zero
SELECT count(*) FROM table_1;
SELECT * FROM relation_accesses WHERE table_name = 'table_1';

-- a very simple test that first checks sequential
-- and parallel SELECTs,DMLs, and DDLs
BEGIN;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	SELECT count(*) FROM table_1 WHERE key = 1;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	SELECT count(*) FROM table_1 WHERE key = 1 OR key = 2;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	INSERT INTO table_1 VALUES (1,1);
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	INSERT INTO table_1 VALUES (1,1), (2,2);
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	ALTER TABLE table_1 ADD COLUMN test_col INT;

	-- now see that the other tables are not accessed at all
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';

ROLLBACK;


-- this test shows that even if two multiple single shard
-- commands executed, we can treat the transaction as sequential
BEGIN;
	SELECT count(*) FROM table_1 WHERE key = 1;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	SELECT count(*) FROM table_1 WHERE key = 2;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	INSERT INTO table_1 VALUES (1,1);
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	INSERT INTO table_1 VALUES (2,2);
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
ROLLBACK;

-- a sample DDL example
BEGIN;
	ALTER TABLE table_1 ADD CONSTRAINT table_1_u UNIQUE (key);
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
ROLLBACK;

-- a simple join touches single shard per table
BEGIN;
	SELECT
		count(*)
	FROM
		table_1, table_2, table_3, table_4, table_5
	WHERE
		table_1.key = table_2.key AND table_2.key = table_3.key AND
		table_3.key = table_4.key AND table_4.key = table_5.key AND
		table_1.key = 1;

		SELECT * FROM relation_accesses WHERE table_name LIKE 'table_%' ORDER BY 1;
ROLLBACK;

-- a simple real-time join touches all shard per table
BEGIN;
	SELECT
		count(*)
	FROM
		table_1, table_2
	WHERE
		table_1.key = table_2.key;

		SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- a simple real-time join touches all shard per table
-- in sequential mode
BEGIN;

	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT
		count(*)
	FROM
		table_1, table_2
	WHERE
		table_1.key = table_2.key;

		SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- a simple subquery pushdown that touches all shards
BEGIN;
	SELECT
		count(*)
	FROM
	(

		SELECT
			random()
		FROM
			table_1, table_2, table_3, table_4, table_5
		WHERE
			table_1.key = table_2.key AND table_2.key = table_3.key AND
			table_3.key = table_4.key AND table_4.key = table_5.key
	) as foo;

	SELECT * FROM relation_accesses WHERE table_name LIKE 'table_%' ORDER BY 1;
ROLLBACK;

-- simple multi shard update both sequential and parallel modes
-- note that in multi shard modify mode we always add select
-- access for all the shards accessed. But, sequential mode is OK
BEGIN;
	UPDATE table_1 SET value = 15;
	SELECT * FROM relation_accesses WHERE table_name = 'table_1';
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';
	UPDATE table_2 SET value = 15;
	SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- now UPDATE/DELETE with subselect pushdown
BEGIN;
	UPDATE
		table_1 SET value = 15
	WHERE key IN (SELECT key FROM table_2 JOIN table_3 USING (key) WHERE table_2.value = 15);
	SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2', 'table_3')  ORDER BY 1;
ROLLBACK;

-- INSERT .. SELECT pushdown
BEGIN;
	INSERT INTO table_2 SELECT * FROM table_1;
	SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- INSERT .. SELECT pushdown in sequential mode should be OK
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';

	INSERT INTO table_2 SELECT * FROM table_1;
	SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- coordinator INSERT .. SELECT
BEGIN;
    -- We use offset 1 to make sure the result needs to be pulled to the coordinator, offset 0 would be optimized away
	INSERT INTO table_2 SELECT * FROM table_1 OFFSET 1;
	SELECT * FROM relation_accesses WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;



-- recursively planned SELECT
BEGIN;
	SELECT
		count(*)
	FROM
	(

		SELECT
			random()
		FROM
			table_1, table_2
		WHERE
			table_1.key = table_2.key
			OFFSET 0
	) as foo;

	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- recursively planned SELECT and coordinator INSERT .. SELECT
BEGIN;
	INSERT INTO table_3 (key)
	SELECT
		*
	FROM
	(

		SELECT
			random() * 1000
		FROM
			table_1, table_2
		WHERE
			table_1.key = table_2.key
			OFFSET 0
	) as foo;

	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2', 'table_3')  ORDER BY 1;
ROLLBACK;

-- recursively planned SELECT and coordinator INSERT .. SELECT
-- but modifies single shard, marked as sequential operation
BEGIN;
	INSERT INTO table_3 (key)
	SELECT
		*
	FROM
	(

		SELECT
			random() * 1000
		FROM
			table_1, table_2
		WHERE
			table_1.key = table_2.key
		AND table_1.key = 1
			OFFSET 0
	) as foo;

	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2', 'table_3')  ORDER BY 1;
ROLLBACK;

-- recursively planned SELECT and recursively planned multi-shard DELETE
BEGIN;
	DELETE FROM table_3 where key IN
	(
		SELECT
			*
		FROM
		(
			SELECT
				table_1.key
			FROM
				table_1, table_2
			WHERE
				table_1.key = table_2.key
				OFFSET 0
		) as foo
	) AND value IN (SELECT key FROM table_4);

	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2', 'table_3', 'table_4')  ORDER BY 1;
ROLLBACK;

-- copy out
BEGIN;
	COPY (SELECT * FROM table_1 WHERE key IN (1,2,3) ORDER BY 1) TO stdout;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- copy in
BEGIN;
	COPY table_1 FROM STDIN WITH CSV;
1,1
2,2
3,3
\.
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- copy in single shard
BEGIN;
	COPY table_1 FROM STDIN WITH CSV;
1,1
1,2
1,3
\.
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- reference table accesses should always be a sequential
BEGIN;
	SELECT count(*) FROM table_6;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_6');

	UPDATE table_6 SET value = 15;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_6');

	ALTER TABLE table_6 ADD COLUMN x INT;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_6');
ROLLBACK;

-- reference table join with a distributed table
BEGIN;
	SELECT count(*) FROM table_1 JOIN table_6 USING(key);
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_6', 'table_1') ORDER BY 1,2;
ROLLBACK;

-- TRUNCATE should be DDL
BEGIN;
	TRUNCATE table_1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- TRUNCATE can be a sequential DDL
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';
	TRUNCATE table_1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- TRUNCATE on a reference table should be sequential
BEGIN;
	TRUNCATE table_6;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_6')  ORDER BY 1;
ROLLBACK;

-- creating foreign keys should consider adding the placement accesses for the referenced table
ALTER TABLE table_1 ADD CONSTRAINT table_1_u UNIQUE (key);
BEGIN;
	ALTER TABLE table_2 ADD CONSTRAINT table_2_u FOREIGN KEY (key) REFERENCES table_1(key);
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- creating foreign keys should consider adding the placement accesses for the referenced table
-- in sequential mode as well
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';

	ALTER TABLE table_2 ADD CONSTRAINT table_2_u FOREIGN KEY (key) REFERENCES table_1(key);
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
SELECT create_distributed_table('partitioning_test', 'id');

-- Adding partition tables via CREATE TABLE should have DDL access the partitioned table as well
BEGIN;
	CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009')  ORDER BY 1;
ROLLBACK;

-- Adding partition tables via ATTACH PARTITION on local tables should have DDL access the partitioned table as well
CREATE TABLE partitioning_test_2009 AS SELECT * FROM partitioning_test;
BEGIN;
	ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2009 FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009')  ORDER BY 1;
COMMIT;

-- Adding partition tables via ATTACH PARTITION on distributed tables should have DDL access the partitioned table as well
CREATE TABLE partitioning_test_2010 AS SELECT * FROM partitioning_test;
SELECT create_distributed_table('partitioning_test_2010', 'id');
BEGIN;
	ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2010 FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- reading from partitioned table marks all of its partitions
BEGIN;
	SELECT count(*) FROM partitioning_test;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- reading from partitioned table sequentially marks all of its partitions with sequential accesses
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM partitioning_test;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- updating partitioned table marks all of its partitions
BEGIN;
	UPDATE partitioning_test SET time = now();
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- updating partitioned table sequentially marks all of its partitions with sequential accesses
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	UPDATE partitioning_test SET time = now();
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;


-- DDLs on partitioned table marks all of its partitions
BEGIN;
	ALTER TABLE partitioning_test ADD COLUMN X INT;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
ROLLBACK;

-- DDLs on partitioned table sequentially marks all of its partitions with sequential accesses
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	ALTER TABLE partitioning_test ADD COLUMN X INT;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
ROLLBACK;


-- reading from partition table marks its parent
BEGIN;
	SELECT count(*) FROM partitioning_test_2009;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- rreading from partition table marks its parent with sequential accesses
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM partitioning_test_2009;
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- updating from partition table marks its parent
BEGIN;
	UPDATE partitioning_test_2009 SET time = now();
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;

-- updating from partition table marks its parent sequential accesses
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	UPDATE partitioning_test_2009 SET time = now();
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
COMMIT;


-- DDLs on partition table marks its parent
BEGIN;
	CREATE INDEX i1000000 ON partitioning_test_2009 (id);
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
ROLLBACK;

-- DDLs on partition table marks its parent in sequential mode
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	CREATE INDEX i1000000 ON partitioning_test_2009 (id);
	SELECT * FROM relation_accesses  WHERE table_name IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')  ORDER BY 1;
ROLLBACK;

-- TRUNCATE CASCADE works fine
ALTER TABLE table_2 ADD CONSTRAINT table_2_u FOREIGN KEY (key) REFERENCES table_1(key);
BEGIN;
	TRUNCATE table_1 CASCADE;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1', 'table_2')  ORDER BY 1;
ROLLBACK;

-- CTEs with SELECT only should work fine
BEGIN;

	WITH cte AS (SELECT count(*) FROM table_1)
	SELECT * FROM cte;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
COMMIT;

-- CTEs with SELECT only in sequential mode should work fine
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';

	WITH cte AS (SELECT count(*) FROM table_1)
	SELECT * FROM cte;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
COMMIT;

-- modifying CTEs should work fine with multi-row inserts, which are by default in sequential
BEGIN;

	WITH cte_1 AS (INSERT INTO table_1 VALUES (1000,1000), (1001, 1001), (1002, 1002) RETURNING *)
	SELECT * FROM cte_1 ORDER BY 1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- modifying CTEs should work fine with parallel mode
BEGIN;

	WITH cte_1 AS (UPDATE table_1 SET value = 15 RETURNING *)
	SELECT count(*) FROM cte_1 ORDER BY 1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- modifying CTEs should work fine with sequential mode
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';

	WITH cte_1 AS (UPDATE table_1 SET value = 15 RETURNING *)
	SELECT count(*) FROM cte_1 ORDER BY 1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- router planned modifying CTEs should work fine with parallel mode
BEGIN;

	WITH cte_1 AS (UPDATE table_1 SET value = 15 WHERE key = 6 RETURNING *)
	SELECT count(*) FROM cte_1 ORDER BY 1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- router planned modifying CTEs should work fine with sequential mode
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode = 'sequential';

	WITH cte_1 AS (UPDATE table_1 SET value = 15 WHERE key = 6 RETURNING *)
	SELECT count(*) FROM cte_1 ORDER BY 1;
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_1')  ORDER BY 1;
ROLLBACK;

-- create distributed table with data loading
-- should mark both parallel dml and parallel ddl
DROP TABLE table_3;
CREATE TABLE table_3 (key int, value int);
INSERT INTO table_3 SELECT i, i FROM generate_series(0,100) i;
BEGIN;
	SELECT create_distributed_table('table_3', 'key');
	SELECT * FROM relation_accesses  WHERE table_name IN ('table_3')  ORDER BY 1;
COMMIT;

SET search_path TO 'public';
DROP SCHEMA access_tracking CASCADE;
