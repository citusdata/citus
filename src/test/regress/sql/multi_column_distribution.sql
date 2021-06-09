CREATE SCHEMA multi_column_distribution;
SET search_path TO multi_column_distribution;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 27905500;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 27905500;

create table t(id int, a int);
select create_distributed_table('t', ARRAY['id'], colocate_with := 'none');
select * from pg_dist_partition WHERE NOT (logicalrelid::text LIKE '%.%');

create table t2(id int, id2 int, a int);
select create_distributed_table('t2', ARRAY['id', 'id2'], colocate_with := 'none');

create table t3(id int, id2 int, b int);
select create_distributed_table('t3', ARRAY['id', 'id2'], colocate_with := 't2');
select * from pg_dist_partition WHERE NOT (logicalrelid::text LIKE '%.%');

INSERT INTO t2 VALUES
(1, 1, 1),
(1, 1, 2),
(1, 1, 4),
(2, 3, 4),
(2, 3, 5),
(2, 4, 5)
;

INSERT INTO t3 VALUES
(1, 1, 1),
(2, 2, 2),
(2, 4, 4)
;

-- partitioning by both distribution columns pushes the window function down
SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id, id2 ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;

EXPLAIN SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id, id2 ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;

-- partitioning by one of the distribution causes the window function not to be
-- pushed down.
SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;

EXPLAIN SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;

SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id2 ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;

EXPLAIN SELECT id, id2, a, rnk FROM
(
    SELECT
      DISTINCT id, id2, a, rank() OVER (PARTITION BY id2 ORDER BY a) as rnk
    FROM
      t2
) as foo
ORDER BY
  rnk, id, id2, a;


-- Can pushdown if joining on both distribution columns
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id = t3.id AND t2.id2 = t3.id2
) foo
ORDER BY 1, 2, 3, 4;

EXPLAIN
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id = t3.id AND t2.id2 = t3.id2
) foo
ORDER BY 1, 2, 3, 4;

-- Cannot pushdown if not joining on both distribution columns
-- NOTE: This currently returns a result, because the logical planner will take
-- over and that doesn't know about. In the EXPLAIN below you can see that it's
-- not pushed down.
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id = t3.id
) foo
ORDER BY 1, 2, 3, 4;

EXPLAIN
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id = t3.id
) foo
ORDER BY 1, 2, 3, 4;

-- Cannot pushdown if not joining on both distribution columns
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id2 = t3.id2
) foo
ORDER BY 1, 2, 3, 4;

EXPLAIN
SELECT * FROM (
    SELECT t2.id, t2.id2, a, b
    FROM t2 JOIN t3 ON t2.id2 = t3.id2
) foo
ORDER BY 1, 2, 3, 4;


SET client_min_messages TO WARNING;
DROP SCHEMA multi_column_distribution CASCADE;
