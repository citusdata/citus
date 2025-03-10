-- MERGE command performs a join from data_source to target_table_name
DROP SCHEMA IF EXISTS schema_shard_table1 CASCADE;
DROP SCHEMA IF EXISTS schema_shard_table2 CASCADE;
DROP SCHEMA IF EXISTS schema_shard_table CASCADE;

-- test merge with schema-shard tables
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SET citus.next_shard_id TO 4005000;
SET citus.enable_repartition_joins TO true;

CREATE SCHEMA schema_shard_table;
SET search_path TO schema_shard_table;
CREATE TABLE reference_table(a int, b int);
CREATE TABLE distributed_table(a int, b int);
CREATE TABLE citus_local_table(a int, b int);
CREATE TABLE postgres_local_table(a int, b int);

INSERT INTO reference_table SELECT i, i FROM generate_series(0, 5) i;
INSERT INTO distributed_table SELECT i, i FROM generate_series(3, 8) i;
INSERT INTO citus_local_table SELECT i, i FROM generate_series(0, 10) i;
INSERT INTO postgres_local_table SELECT i, i FROM generate_series(5, 10) i;

SELECT create_reference_table('reference_table');
SELECT create_distributed_table('distributed_table', 'a');
SELECT citus_add_local_table_to_metadata('citus_local_table');

SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA schema_shard_table1;
CREATE SCHEMA schema_shard_table2;

SET search_path TO schema_shard_table1;
CREATE TABLE nullkey_c1_t1(a int, b int);
CREATE TABLE nullkey_c1_t2(a int, b int);
INSERT INTO nullkey_c1_t1 SELECT i, i FROM generate_series(0, 5) i;
INSERT INTO nullkey_c1_t2 SELECT i, i FROM generate_series(3, 8) i;

SET search_path TO schema_shard_table2;
CREATE TABLE nullkey_c2_t1(a int, b int);
CREATE TABLE nullkey_c2_t2(a int, b int);
INSERT INTO nullkey_c2_t1 SELECT i, i FROM generate_series(0, 5) i;
INSERT INTO nullkey_c2_t2 SELECT i, i FROM generate_series(3, 8) i;

SET search_path TO schema_shard_table1;

-- with a colocated table
SET client_min_messages TO DEBUG2;
MERGE INTO nullkey_c1_t1 USING nullkey_c1_t2 ON (nullkey_c1_t1.a = nullkey_c1_t2.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c1_t2.b;

MERGE INTO nullkey_c1_t1 USING nullkey_c1_t2 ON (nullkey_c1_t1.a = nullkey_c1_t2.a)
WHEN MATCHED THEN DELETE;

MERGE INTO nullkey_c1_t1 USING nullkey_c1_t2 ON (nullkey_c1_t1.a = nullkey_c1_t2.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c1_t2.b
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c1_t2.a, nullkey_c1_t2.b);

MERGE INTO nullkey_c1_t1 USING nullkey_c1_t2 ON (nullkey_c1_t1.a = nullkey_c1_t2.a)
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c1_t2.a, nullkey_c1_t2.b);

SET search_path TO schema_shard_table2;

-- with non-colocated schema-shard table
MERGE INTO schema_shard_table1.nullkey_c1_t1 USING nullkey_c2_t1 ON (schema_shard_table1.nullkey_c1_t1.a = nullkey_c2_t1.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c2_t1.b;

MERGE INTO schema_shard_table1.nullkey_c1_t1 USING nullkey_c2_t1 ON (schema_shard_table1.nullkey_c1_t1.a = nullkey_c2_t1.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c2_t1.b
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c2_t1.a, nullkey_c2_t1.b);

-- with a distributed table
SET search_path TO schema_shard_table1;
MERGE INTO nullkey_c1_t1 USING schema_shard_table.distributed_table ON (nullkey_c1_t1.a = schema_shard_table.distributed_table.a)
WHEN MATCHED THEN UPDATE SET b = schema_shard_table.distributed_table.b
WHEN NOT MATCHED THEN INSERT VALUES (schema_shard_table.distributed_table.a, schema_shard_table.distributed_table.b);

MERGE INTO schema_shard_table.distributed_table USING nullkey_c1_t1 ON (nullkey_c1_t1.a = schema_shard_table.distributed_table.a)
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c1_t1.a, nullkey_c1_t1.b);

RESET client_min_messages;
SELECT count(*) FROM schema_shard_table.distributed_table WHERE a in (0, 1, 2);
MERGE INTO schema_shard_table.distributed_table
USING (SELECT s1.a AS s1a, s2.b AS s2b
	FROM nullkey_c1_t1 s1 JOIN schema_shard_table2.nullkey_c2_t1 s2
	ON s1.a = s2.a) src
ON (src.s1a = schema_shard_table.distributed_table.a)
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (src.s1a, src.s2b);
-- Three matching rows must be deleted
SELECT count(*) FROM schema_shard_table.distributed_table WHERE a in (0, 1, 2);

-- with a reference table
SET client_min_messages TO DEBUG2;
MERGE INTO nullkey_c1_t1 USING schema_shard_table.reference_table ON (nullkey_c1_t1.a = schema_shard_table.reference_table.a)
WHEN MATCHED THEN UPDATE SET b = schema_shard_table.reference_table.b;

MERGE INTO schema_shard_table.reference_table USING nullkey_c1_t1 ON (nullkey_c1_t1.a = schema_shard_table.reference_table.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c1_t1.b
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c1_t1.a, nullkey_c1_t1.b);

-- with a citus local table
MERGE INTO nullkey_c1_t1 USING schema_shard_table.citus_local_table ON (nullkey_c1_t1.a = schema_shard_table.citus_local_table.a)
WHEN MATCHED THEN UPDATE SET b = schema_shard_table.citus_local_table.b;

MERGE INTO schema_shard_table.citus_local_table USING nullkey_c1_t1 ON (nullkey_c1_t1.a = schema_shard_table.citus_local_table.a)
WHEN MATCHED THEN DELETE;

-- with a postgres table
MERGE INTO nullkey_c1_t1 USING schema_shard_table.postgres_local_table ON (nullkey_c1_t1.a = schema_shard_table.postgres_local_table.a)
WHEN MATCHED THEN UPDATE SET b = schema_shard_table.postgres_local_table.b;

MERGE INTO schema_shard_table.postgres_local_table USING nullkey_c1_t1 ON (nullkey_c1_t1.a = schema_shard_table.postgres_local_table.a)
WHEN MATCHED THEN UPDATE SET b = nullkey_c1_t1.b
WHEN NOT MATCHED THEN INSERT VALUES (nullkey_c1_t1.a, nullkey_c1_t1.b);

-- using ctes
WITH cte AS (
    SELECT * FROM nullkey_c1_t1
)
MERGE INTO nullkey_c1_t1 USING cte ON (nullkey_c1_t1.a = cte.a)
WHEN MATCHED THEN UPDATE SET b = cte.b;

WITH cte AS (
    SELECT * FROM schema_shard_table.distributed_table
)
MERGE INTO nullkey_c1_t1 USING cte ON (nullkey_c1_t1.a = cte.a)
WHEN MATCHED THEN UPDATE SET b = cte.b;

WITH cte AS materialized (
    SELECT * FROM schema_shard_table.distributed_table
)
MERGE INTO nullkey_c1_t1 USING cte ON (nullkey_c1_t1.a = cte.a)
WHEN MATCHED THEN UPDATE SET b = cte.b;

SET client_min_messages TO WARNING;
DROP SCHEMA schema_shard_table1 CASCADE;
DROP SCHEMA schema_shard_table2 CASCADE;
DROP SCHEMA schema_shard_table CASCADE;
