\SET VERBOSITY terse

SET citus.next_shard_id TO 1513000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA mixed_relkind_tests;
SET search_path TO mixed_relkind_tests;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages TO ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- make results consistent

-- create test tables
CREATE TABLE postgres_local_table (a int);

CREATE TABLE partitioned_postgres_local_table(a int) PARTITION BY RANGE(a);
CREATE TABLE partitioned_postgres_local_table_1 PARTITION OF partitioned_postgres_local_table FOR VALUES FROM (0) TO (3);
CREATE TABLE partitioned_postgres_local_table_2 PARTITION OF partitioned_postgres_local_table FOR VALUES FROM (3) TO (1000);

CREATE TABLE reference_table(a int);
SELECT create_reference_table('reference_table');

CREATE VIEW view_on_ref AS SELECT * FROM reference_table;

CREATE TABLE citus_local_table(a int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

CREATE VIEW view_on_citus_local AS SELECT * FROM citus_local_table;

CREATE UNLOGGED TABLE unlogged_distributed_table(a int, b int);
SELECT create_distributed_table('unlogged_distributed_table', 'a');

CREATE TABLE distributed_table(a int);
SELECT create_distributed_table('distributed_table', 'a');

CREATE VIEW view_on_dist AS SELECT * FROM distributed_table;
CREATE MATERIALIZED VIEW mat_view_on_dist AS SELECT * FROM distributed_table;

CREATE TABLE partitioned_distributed_table(a int, b int) PARTITION BY RANGE(a);
CREATE TABLE partitioned_distributed_table_1 PARTITION OF partitioned_distributed_table FOR VALUES FROM (0) TO (3);
CREATE TABLE partitioned_distributed_table_2 PARTITION OF partitioned_distributed_table FOR VALUES FROM (3) TO (1000);
SELECT create_distributed_table('partitioned_distributed_table', 'a');

CREATE VIEW view_on_part_dist AS SELECT * FROM partitioned_distributed_table;
CREATE MATERIALIZED VIEW mat_view_on_part_dist AS SELECT * FROM partitioned_distributed_table;

-- and insert some data
INSERT INTO postgres_local_table SELECT * FROM generate_series(0, 5);
INSERT INTO partitioned_postgres_local_table SELECT * FROM generate_series(0, 5);
INSERT INTO reference_table SELECT * FROM generate_series(0, 5);
INSERT INTO citus_local_table SELECT * FROM generate_series(0, 5);
INSERT INTO unlogged_distributed_table SELECT a,a+1 FROM generate_series(0, 5) AS a;
INSERT INTO distributed_table SELECT * FROM generate_series(0, 5);
INSERT INTO partitioned_distributed_table SELECT a,a+1 FROM generate_series(0, 5) AS a;

-- should work
SELECT * FROM partitioned_distributed_table UNION SELECT 1,1 ORDER BY 1,2;
SELECT * FROM partitioned_distributed_table UNION SELECT 1, * FROM postgres_local_table ORDER BY 1,2;
SELECT * FROM partitioned_distributed_table UNION SELECT * FROM unlogged_distributed_table ORDER BY 1,2;
SELECT *, 1 FROM postgres_local_table UNION SELECT * FROM unlogged_distributed_table ORDER BY 1,2;
SELECT * FROM unlogged_distributed_table UNION SELECT 1,1 ORDER BY 1,2;
SELECT 1 UNION SELECT * FROM citus_local_table ORDER BY 1;

SELECT * FROM view_on_part_dist UNION SELECT 1,1 ORDER BY 1,2;
SELECT * FROM mat_view_on_part_dist UNION SELECT 1,1 ORDER BY 1,2;
SELECT * FROM view_on_citus_local UNION SELECT 1 ORDER BY 1;
SELECT * FROM view_on_dist UNION SELECT 1 ORDER BY 1;
SELECT * FROM mat_view_on_dist UNION SELECT 1 ORDER BY 1;

SET client_min_messages TO DEBUG1;

-- can push down the union in subquery
SELECT * FROM (SELECT * FROM partitioned_distributed_table UNION SELECT * FROM partitioned_distributed_table) AS foo ORDER BY 1,2;

-- cannot push down the subquery, should evaluate subquery by creating a subplan
SELECT COUNT(*) FROM (SELECT b, random() FROM partitioned_distributed_table GROUP BY b) AS foo;
SELECT * FROM partitioned_distributed_table WHERE b IN (SELECT a FROM postgres_local_table) ORDER BY 1,2;

-- can push down the subquery
SELECT * FROM partitioned_distributed_table WHERE a IN (SELECT a FROM distributed_table) ORDER BY 1,2;
SELECT * FROM partitioned_distributed_table WHERE a IN (SELECT a FROM view_on_part_dist) ORDER BY 1,2;
SELECT * FROM distributed_table WHERE a IN (SELECT a FROM view_on_part_dist) ORDER BY 1;
SELECT * FROM view_on_dist WHERE a IN (SELECT a FROM view_on_part_dist) ORDER BY 1;
SELECT * FROM view_on_citus_local WHERE a IN (SELECT a FROM reference_table) ORDER BY 1;
SELECT COUNT(*) FROM (SELECT a, random() FROM partitioned_distributed_table GROUP BY a) AS foo;

-- should add (a IS NOT NULL) filters similar to regular distributed tables
RESET client_min_messages;
SELECT public.explain_has_is_not_null(
$$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT * FROM partitioned_distributed_table;
$$);
SET client_min_messages TO DEBUG1;

SELECT COUNT(*) FROM partitioned_postgres_local_table JOIN distributed_table ON (true);
SELECT COUNT(*) FROM partitioned_postgres_local_table JOIN partitioned_distributed_table ON (true);
SELECT COUNT(*) FROM distributed_table JOIN partitioned_postgres_local_table ON (true);
INSERT INTO partitioned_distributed_table SELECT foo.* FROM partitioned_distributed_table AS foo JOIN citus_local_table ON (true);
INSERT INTO partitioned_distributed_table SELECT foo.* FROM distributed_table AS foo JOIN citus_local_table ON (true);
INSERT INTO distributed_table SELECT foo.a FROM partitioned_distributed_table AS foo JOIN citus_local_table ON (true);

SELECT COUNT(*) FROM reference_table LEFT JOIN partitioned_distributed_table ON true;

-- non-colocated subquery should work
SELECT COUNT(*) FROM
  (SELECT *, random() FROM partitioned_distributed_table) AS foo,
  (SELECT *, random() FROM partitioned_distributed_table) AS bar
WHERE foo.a = bar.b;

UPDATE partitioned_distributed_table SET b = foo.a FROM citus_local_table AS foo;
UPDATE partitioned_distributed_table SET b = foo.a FROM postgres_local_table AS foo;
UPDATE partitioned_distributed_table SET a = foo.a FROM postgres_local_table AS foo WHERE foo.a = partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM citus_local_table AS foo WHERE foo.a = partitioned_distributed_table.a;
-- should fail
UPDATE partitioned_distributed_table SET a = foo.a FROM mat_view_on_part_dist AS foo WHERE foo.a = partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM partitioned_distributed_table AS foo WHERE foo.a < partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM distributed_table AS foo WHERE foo.a < partitioned_distributed_table.a;

-- should work
UPDATE partitioned_distributed_table SET a = foo.a FROM partitioned_distributed_table AS foo WHERE foo.a = partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM view_on_part_dist AS foo WHERE foo.a = partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM view_on_dist AS foo WHERE foo.a = partitioned_distributed_table.a;
UPDATE partitioned_distributed_table SET a = foo.a FROM view_on_ref AS foo WHERE foo.a = partitioned_distributed_table.a;

-- JOINs on the distribution key
SELECT COUNT(*) FROM partitioned_distributed_table p1 JOIN partitioned_distributed_table p2 USING (a);
SELECT COUNT(*) FROM unlogged_distributed_table u1 JOIN partitioned_distributed_table p2 USING (a);
SELECT COUNT(*) FROM partitioned_distributed_table p1 LEFT JOIN partitioned_distributed_table p2 USING (a);

-- lateral JOIN
SELECT COUNT(*) FROM partitioned_distributed_table p1 JOIN LATERAL (SELECT * FROM partitioned_distributed_table p2 WHERE p1.a = p2.a) AS foo ON (true);

-- router query
SELECT COUNT(*) FROM partitioned_distributed_table p1 JOIN partitioned_distributed_table p2 USING (a) WHERE a = 1;

-- repartition query
SET citus.enable_repartition_joins TO ON;
SELECT COUNT(*) FROM partitioned_distributed_table p1 JOIN partitioned_distributed_table p2 USING (b) WHERE b = 1;
SELECT COUNT(*) FROM unlogged_distributed_table u1 JOIN partitioned_distributed_table p2 USING (b) WHERE b = 1;
RESET citus.enable_repartition_joins;

-- joins with cte's
WITH cte_1 AS MATERIALIZED (SELECT * FROM partitioned_distributed_table)
  SELECT COUNT(*) FROM cte_1;

WITH cte_1 AS MATERIALIZED (SELECT * FROM partitioned_distributed_table)
  SELECT COUNT(*) FROM cte_1 JOIN partitioned_distributed_table USING (a);

WITH cte_1 AS MATERIALIZED (SELECT * FROM partitioned_distributed_table)
  SELECT COUNT(*) FROM cte_1 JOIN partitioned_distributed_table USING (b);

-- multi shard colocated update
UPDATE partitioned_distributed_table dt
SET b = sub1.a + sub2.a
FROM (SELECT * FROM partitioned_distributed_table WHERE b = 1) AS sub1,
     (SELECT * FROM partitioned_distributed_table WHERE b = 2) AS sub2
WHERE sub1.a = sub2.a AND sub1.a = dt.a AND dt.a > 1;

UPDATE unlogged_distributed_table dt
SET b = sub1.a + sub2.a
FROM (SELECT * FROM unlogged_distributed_table WHERE b = 1) AS sub1,
     (SELECT * FROM unlogged_distributed_table WHERE b = 2) AS sub2
WHERE sub1.a = sub2.a AND sub1.a = dt.a AND dt.a > 1;

-- multi shard non-colocated update
WITH cte1 AS MATERIALIZED (SELECT * FROM partitioned_distributed_table WHERE b = 1),
     cte2 AS MATERIALIZED (SELECT * FROM partitioned_distributed_table WHERE b = 2)
UPDATE partitioned_distributed_table dt SET b = cte1.a + cte2.a
FROM cte1, cte2 WHERE cte1.a != cte2.a AND cte1.a = dt.a AND dt.a > 1;

-- router update with CTE
UPDATE partitioned_distributed_table dt
SET b = sub1.a + sub2.a
FROM (SELECT * FROM partitioned_distributed_table WHERE b = 1) AS sub1,
     (SELECT * FROM partitioned_distributed_table WHERE b = 2) AS sub2
WHERE sub1.a = sub2.a AND sub1.a = dt.a AND dt.a = 1;

-- INSERT .. SELECT via coordinator
RESET client_min_messages;

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT * FROM partitioned_distributed_table ORDER BY 1,2 LIMIT 5;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO unlogged_distributed_table SELECT * FROM partitioned_distributed_table ORDER BY 1,2 LIMIT 5;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT * FROM distributed_table ORDER BY 1 LIMIT 5;
$Q$);

-- INSERT .. SELECT via repartition
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT a + 1 FROM partitioned_distributed_table;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO unlogged_distributed_table SELECT a + 1 FROM partitioned_distributed_table;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT a + 1 FROM distributed_table;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_distributed_table SELECT a + 1 FROM unlogged_distributed_table;
$Q$);

SET client_min_messages TO DEBUG1;

-- some aggregate queries
SELECT sum(a) FROM partitioned_distributed_table;
SELECT ceil(regr_syy(a, b)) FROM partitioned_distributed_table;
SELECT ceil(regr_syy(a, b)) FROM unlogged_distributed_table;

-- pushdown WINDOW
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
SELECT a, COUNT(*) OVER (PARTITION BY a) FROM partitioned_distributed_table ORDER BY 1,2;
$Q$);

-- pull to coordinator WINDOW
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
SELECT a, COUNT(*) OVER (PARTITION BY a+1) FROM partitioned_distributed_table ORDER BY 1,2;
$Q$);

-- FOR UPDATE
SELECT * FROM partitioned_distributed_table WHERE a = 1 ORDER BY 1,2 FOR UPDATE;
SELECT * FROM unlogged_distributed_table WHERE a = 1 ORDER BY 1,2 FOR UPDATE;

VACUUM partitioned_distributed_table;
TRUNCATE partitioned_distributed_table;

SET client_min_messages TO ERROR;

-- drop column followed by SELECT in transaction block
BEGIN;
  ALTER TABLE partitioned_distributed_table DROP COLUMN b CASCADE;
  SELECT * FROM partitioned_distributed_table;
COMMIT;

-- cleanup at exit
DROP SCHEMA mixed_relkind_tests CASCADE;
