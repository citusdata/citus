CREATE SCHEMA recurring_outer_join;
SET search_path TO recurring_outer_join;

SET citus.next_shard_id TO 1520000;
SET citus.shard_count TO 32;

SET client_min_messages TO DEBUG1;
CREATE TABLE dist_1 (a int, b int);
SELECT create_distributed_table('dist_1', 'a');
INSERT INTO dist_1 VALUES
(1, 10),
(1, 11),
(1, 12),
(2, 20),
(2, 21),
(2, 22),
(2, 23),
(3, 30),
(3, 31),
(3, 32),
(3, 33),
(3, 34),
(7, 40),
(7, 41),
(7, 42);

CREATE TABLE dist_1_local(LIKE dist_1);
INSERT INTO dist_1_local SELECT * FROM dist_1;

CREATE TABLE dist_2_columnar(LIKE dist_1) USING columnar;
INSERT INTO dist_2_columnar SELECT * FROM dist_1;
SELECT create_distributed_table('dist_2_columnar', 'a');

CREATE TABLE dist_3_partitioned(LIKE dist_1) PARTITION BY RANGE(a);
CREATE TABLE dist_3_partitioned_p1 PARTITION OF dist_3_partitioned FOR VALUES FROM (0) TO (2);
CREATE TABLE dist_3_partitioned_p2 PARTITION OF dist_3_partitioned FOR VALUES FROM (2) TO (4);
CREATE TABLE dist_3_partitioned_p3 PARTITION OF dist_3_partitioned FOR VALUES FROM (4) TO (100);
SELECT create_distributed_table('dist_3_partitioned', 'a');
INSERT INTO dist_3_partitioned SELECT * FROM dist_1;

CREATE TABLE dist_4 (a int, b int);
SELECT create_distributed_table('dist_4', 'a');
INSERT INTO dist_4 VALUES
(1, 100),
(1, 101),
(1, 300),
(2, 20),
(2, 21),
(2, 400),
(2, 23),
(3, 102),
(3, 301),
(3, 300),
(3, null),
(3, 34),
(7, 40),
(7, null),
(7, 11);

CREATE TABLE dist_4_local(LIKE dist_4);
INSERT INTO dist_4_local SELECT * FROM dist_4;

CREATE TABLE ref_1 (a int, b int);
SELECT create_reference_table('ref_1');
INSERT INTO ref_1 VALUES
(1, 100),
(1, 11),
(null, 102),
(2, 200),
(2, 21),
(null, 202),
(2, 203),
(4, 300),
(4, 301),
(null, 302),
(4, 303),
(4, 304),
(null, 400),
(null, 401),
(null, 402);

CREATE TABLE ref_1_local(LIKE ref_1);
INSERT INTO ref_1_local SELECT * FROM ref_1;

--- We create a second reference table that does not have the distribution column
CREATE TABLE ref_2 (a2 int, b int);
SELECT create_reference_table('ref_2');
INSERT INTO ref_2 VALUES
(1, null),
(1, 100),
(1, 11),
(null, 102),
(2, 200),
(2, 21),
(null, 202),
(2, 203),
(4, 300),
(4, 301),
(null, 302),
(4, 303),
(4, 304),
(null, 400),
(null, 401),
(null, 402);

CREATE TABLE ref_2_local(LIKE ref_2); 
INSERT INTO ref_2_local SELECT * FROM ref_2;

CREATE TABLE local_1 (a int, b int);
INSERT INTO local_1 VALUES
(null, 1000),
(1, 11),
(1, 100),
(5, 2000),
(5, 2001),
(5, 2002),
(null, 2003),
(6, 3000),
(6, 3001),
(6, 3002),
(null, 3003),
(6, 3004),
(null, 4000),
(null, 4001),
(null, 4002);

CREATE TABLE citus_local_1(LIKE local_1);
INSERT INTO citus_local_1 SELECT * FROM local_1;
SELECT citus_add_local_table_to_metadata('citus_local_1');

CREATE TABLE dist_4_different_colocation_group(LIKE dist_1);
INSERT INTO dist_4_different_colocation_group SELECT * FROM local_1;
DELETE FROM dist_4_different_colocation_group WHERE a IS NULL;
SELECT create_distributed_table('dist_4_different_colocation_group', 'a', colocate_with=>'none');

CREATE TABLE dist_5_with_pkey(LIKE dist_1);
INSERT INTO dist_5_with_pkey VALUES
(1, 11),
(2, 22),
(3, 34),
(7, 40);
SELECT create_distributed_table('dist_5_with_pkey', 'a');
ALTER TABLE dist_5_with_pkey ADD CONSTRAINT pkey_1 PRIMARY KEY (a);

--
-- basic cases
--

SELECT COUNT(*) FROM ref_1 LEFT JOIN dist_1 USING (a);
SELECT COUNT(*) FROM ref_1_local LEFT JOIN dist_1_local USING (a);

SELECT COUNT(*) FROM ref_1 LEFT JOIN dist_1 USING (a,b);
SELECT COUNT(*) FROM ref_1_local LEFT JOIN dist_1_local USING (a,b);

SELECT COUNT(*) FROM ref_1 LEFT JOIN dist_4 USING (b);
SELECT COUNT(*) FROM ref_1_local LEFT JOIN dist_4_local USING (b);

SELECT * FROM ref_2 LEFT JOIN dist_4 USING (b) ORDER BY b, a2, a;
SELECT * FROM ref_2_local LEFT JOIN dist_4_local USING (b) ORDER BY b, a2, a;

SELECT COUNT(*) FROM dist_1 RIGHT JOIN ref_1 USING (a);

SELECT COUNT(*) FROM ref_1 FULL JOIN dist_1 USING (a);
SELECT COUNT(*) FROM dist_1 FULL JOIN ref_1 USING (a);

SELECT COUNT(*) FROM dist_1 FULL JOIN ref_1 USING (a,b);

-- distributed side is a subquery
SELECT COUNT(*) FROM ref_1 LEFT JOIN (SELECT * FROM dist_1) q USING (a);

-- distributed side is a join tree
SELECT COUNT(*) FROM ref_1 LEFT JOIN (dist_1 t1 JOIN dist_1 t2 USING (a)) q USING (a);
SELECT COUNT(*) FROM ref_1 LEFT JOIN (dist_1 t1 LEFT JOIN dist_1 t2 USING (a)) q USING (a);

-- use functions/VALUES clauses/intrermediate results as the recurring rel

  -- values clause
  SELECT COUNT(*) FROM (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) recurring LEFT JOIN dist_1 USING (a);

  -- generate_series()
  SELECT COUNT(*) FROM dist_1 RIGHT JOIN (SELECT a FROM generate_series(1, 10) a) recurring USING (a);

  -- materialized cte
  WITH dist_1_materialized AS MATERIALIZED (
    SELECT * FROM dist_1
  )
  SELECT COUNT(*) FROM dist_1 RIGHT JOIN dist_1_materialized USING (a);

  -- offset in the subquery
  SELECT COUNT(*) FROM dist_1 t1 RIGHT JOIN (SELECT * FROM dist_1 OFFSET 0) t2 USING (a);

  -- limit in the subquery
  SELECT COUNT(*) FROM dist_1 t1 RIGHT JOIN (SELECT * FROM dist_1 ORDER BY 1,2 LIMIT 2) t2 USING (a);

  -- local-distributed join as the recurring rel
  --
  -- We plan local-distributed join by converting local_1 into an intermediate result
  -- and hence it becomes a recurring rel. Then we convert distributed - inner side of
  -- the right join (dist_1) into an intermediate result too and this makes rhs of the
  -- full join a recurring rel. And finally, we convert lhs of the full join (t1) into
  -- an intermediate result too.
  SELECT COUNT(*) FROM dist_1 t1 FULL JOIN (dist_1 RIGHT JOIN local_1 USING(a)) t2 USING (a);

  SELECT COUNT(*) FROM dist_1 t1 FULL JOIN (dist_1 RIGHT JOIN citus_local_1 USING(a)) t2 USING (a);

  -- subqury without FROM
  SELECT COUNT(*) FROM dist_1 t1 RIGHT JOIN (SELECT generate_series(1,10) AS a) t2 USING (a);

-- such semi joins / anti joins are supported too

  -- reference table
  SELECT COUNT(*) FROM
  ref_1 t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

  -- not supported because we join t3 (inner rel of the anti join) with a column
  -- of reference table, not with the distribution column of the other distributed
  -- table (t2)
  SELECT COUNT(*) FROM
  ref_1 t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE NOT EXISTS (SELECT * FROM dist_1 t3 WHERE t1.a = a);

  -- supported because the semi join is performed based on distribution keys
  -- of the distributed tables
  SELECT COUNT(*) FROM
  ref_1 t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE NOT EXISTS (SELECT * FROM dist_1 t3 WHERE t2.a = a);

  -- values clause
  SELECT COUNT(*) FROM
  (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE EXISTS (SELECT * FROM dist_1 t3 WHERE t1.a = a);

  -- offset in the subquery
  SELECT COUNT(*) FROM
  (SELECT * FROM dist_1 OFFSET 0) t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

  -- local-distributed join as the recurring rel
  SELECT COUNT(*) FROM
  (dist_1 RIGHT JOIN local_1 USING(a)) t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

  -- materialized cte
  WITH dist_1_materialized AS MATERIALIZED (
      SELECT * FROM dist_1
  )
  SELECT COUNT(*) FROM
  dist_1_materialized t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

  WITH dist_1_materialized AS MATERIALIZED (
      SELECT * FROM dist_1
  )
  SELECT COUNT(*) FROM
  dist_1_materialized t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE EXISTS (SELECT a FROM dist_1 t3 WHERE t3.a = t1.a);

  -- not supported because we anti-join t3 --inner rel-- with a column
  -- of t1 (intermediate result) --outer-rel--
  WITH dist_1_materialized AS MATERIALIZED (
      SELECT * FROM dist_1
  )
  SELECT COUNT(*) FROM
  dist_1_materialized t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE NOT EXISTS (SELECT a FROM dist_1 t3 WHERE t3.a = t1.a);

  -- so this is supported because now t3 is joined with t2, not t1
  WITH dist_1_materialized AS MATERIALIZED (
      SELECT a AS a_alias, b AS b_alias FROM dist_1
  )
  SELECT COUNT(*) FROM
  dist_1_materialized t1
  JOIN dist_1 t2
  ON (t1.a_alias = t2.a)
  WHERE NOT EXISTS (SELECT a FROM dist_1 t3 WHERE t3.a = t2.a);

  WITH dist_1_materialized AS MATERIALIZED (
      SELECT a AS a_alias, b AS b_alias FROM dist_1
  )
  SELECT COUNT(*) FROM
  dist_1_materialized t1
  JOIN dist_1 t2
  ON (t1.a_alias = t2.a)
  WHERE t1.a_alias NOT IN (SELECT a FROM dist_1 t3);

  -- generate_series()
  SELECT COUNT(*) FROM
  (SELECT a FROM generate_series(1, 10) a) t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

  -- subqury without FROM
  SELECT COUNT(*) FROM
  (SELECT generate_series(1,10) AS a) t1
  JOIN dist_1 t2
  ON (t1.a = t2.a)
  WHERE t1.a IN (SELECT a FROM dist_1 t3);

-- together with correlated subqueries

SELECT COUNT(*) FROM ref_1 t1
LEFT JOIN dist_1 t2 USING (a,b)
WHERE EXISTS (SELECT * FROM dist_1 t3 WHERE t1.a = t3.a);

SELECT COUNT(*) FROM dist_1 t1
RIGHT JOIN ref_1 t2 USING (a,b)
WHERE EXISTS (SELECT * FROM dist_1 t3 WHERE t2.a = t3.a);


-- "dist_1 t2" can't contribute to result set of the right join with
-- a tuple having "(t2.a) a = NULL" because t2 is in the inner side of
-- right join. For this reason, Postgres knows that <t2.a = t1.a> can
-- never evaluate to true (because <NULL = NULL> never yields "true")
-- and replaces the right join with an inner join.
-- And as a result, we can push-down the query without having to go
-- through recursive planning.
SELECT COUNT(*) FROM dist_1 t1
WHERE EXISTS (
    SELECT * FROM dist_1 t2
    RIGHT JOIN ref_1 t3 USING (a)
    WHERE t2.a = t1.a
);

-- same here, Postgres converts the left join into an inner one
SELECT foo.* FROM
ref_1 r1,
LATERAL
(
    SELECT * FROM ref_1 r2
    LEFT JOIN dist_1
    USING (a)
    WHERE r1.a > dist_1.b
) as foo;

-- Qual is the same but top-level join is an anti-join. Right join
-- stays as is and hence requires recursive planning.
SELECT COUNT(*) FROM dist_1 t1
WHERE NOT EXISTS (
    SELECT * FROM dist_1 t2
    RIGHT JOIN ref_1 t3 USING (a)
    WHERE t2.a = t1.a
);

-- This time the semi-join qual is <t3.a = t1.a> (not <<t2.a = t1.a>)
-- where t3 is the outer rel of the right join. Hence Postgres can't
-- replace right join with an inner join and so we recursively plan
-- inner side of the right join since the outer side is a recurring
-- rel.
SELECT COUNT(*) FROM dist_1 t1
WHERE EXISTS (
    SELECT * FROM dist_1 t2
    RIGHT JOIN ref_1 t3 USING (a)
    WHERE t3.a = t1.a
);

SELECT COUNT(*) FROM dist_1 t1
WHERE NOT EXISTS (
    SELECT * FROM dist_1 t2
    RIGHT JOIN ref_1 t3 USING (a)
    WHERE t3.a = t1.a
);

--
-- more complex cases
--

SELECT COUNT(*) FROM
-- 1) right side is distributed but t1 is recurring, hence what
--    makes the right side distributed (t3) is recursively planned
ref_1 t1
LEFT JOIN
(ref_1 t2 RIGHT JOIN dist_1 t3(x,y) ON (t2.a=t3.x)) t5
USING(a)
-- 2) outer side of the join tree became recurring, hence t4 is
--    recursively planned too
LEFT JOIN
dist_1 t4
ON (t4.a = t5.a AND t4.b = t5.b)
WHERE t4.b IS NULL;

SELECT COUNT(*) FROM
-- 2) right side is distributed but t1 is recurring, hence what
--    makes the right side distributed (t4) is recursively planned
ref_1 t1
LEFT JOIN
(
    dist_1 t4
    JOIN
    -- 1) t6 is recursively planned since the outer side is recurring
    (SELECT t6.a FROM dist_1 t6 RIGHT JOIN ref_1 t7 USING(a)) t5
    USING(a)
) q
USING(a)
-- 3) outer side of the join tree became recurring, hence t8 is
--    recursively planned too
LEFT JOIN
dist_1 t8
USING (a)
WHERE t8.b IS NULL;

SELECT COUNT(*) FROM
ref_1 t1
-- all distributed tables in the rhs will be recursively planned
-- in the order of t3, t4, t5
LEFT JOIN
(
    ref_1 t2
    JOIN
    dist_1 t3
    USING (a)
    JOIN
    (dist_1 t4 JOIN dist_1 t5 USING (a))
    USING(a)
)
USING (a);

-- Even if dist_1 and dist_4_different_colocation_group belong to different
-- colocation groups, we can run query without doing a repartition join as
-- we first decide recursively planning lhs of the right join because rhs
-- (ref_1) is a recurring rel. And while doing so, we anyway recursively plan
-- the distributed tables in the subjoin tree individually hence the whole join
-- tree becomes:
--                                          RIGHT JOIN
--                                      /               \
--   intermediate_result_for_dist_1                     ref_1
--   JOIN
--   intermediate_result_for_dist_4_different_colocation_group
--
-- When we decide implementing the optimization noted in
-- RecursivelyPlanDistributedJoinNode in an XXX comment, then this query would
-- require enabling repartition joins.
SELECT COUNT(*) FROM
dist_1 JOIN dist_4_different_colocation_group USING(a)
RIGHT JOIN ref_1 USING(a);

SELECT COUNT(*) FROM
ref_1 t1
LEFT JOIN
-- 2) t6 subquery is distributed so needs to be recursively planned
--    because t1 is recurring
(
    SELECT * FROM
    (SELECT * FROM ref_1 t2 JOIN dist_1 t3 USING (a) WHERE t3.b IS NULL) p
    JOIN
    -- 1) t5 is recursively planned since the outer side is recurring
    (SELECT * FROM ref_1 t4 LEFT JOIN dist_1 t5 USING (a)) q
    USING(a)
) t6
USING (a);

-- No need to recursively plan dist_5_with_pkey thanks to
-- pkey optimizations done by Postgres.
SELECT COUNT(*) FROM ref_1 LEFT JOIN dist_5_with_pkey USING(a);

-- Similarly, <dist_1.a IN (1,4)> implies that "dist_1.a" cannot be NULL
-- and hence Postgres converts the LEFT JOIN into an INNER JOIN form.
-- For this reason, we don't need to recursively plan dist_1.
SELECT COUNT(*) FROM ref_1 LEFT JOIN dist_1 USING(a) WHERE dist_1.a IN (1,4);

SELECT COUNT(*) FROM
ref_1 t1
LEFT JOIN
-- 2) t6 subquery is distributed so needs to be recursively planned
--    because t1 is recurring
(
    SELECT * FROM
    (SELECT * FROM ref_1 t2 JOIN dist_3_partitioned t3 USING (a) WHERE t3.b IS NULL) p
    JOIN
    -- 1) t5 is recursively planned since the outer side is recurring
    (SELECT * FROM ref_1 t4 LEFT JOIN dist_1 t5 USING (a)) q
    USING(a)
) t6
USING (a);

SELECT COUNT(t1.a), t1.b FROM
ref_1 t1
LEFT JOIN
-- 2) t6 subquery is distributed so needs to be recursively planned
--    because t1 is recurring
(
    SELECT * FROM
    (SELECT * FROM ref_1 t2 JOIN dist_3_partitioned t3 USING (a) WHERE t3.b IS NULL) p
    JOIN
    -- 1) t5 is recursively planned since the outer side is recurring
    (SELECT * FROM ref_1 t4 LEFT JOIN dist_1 t5 USING (a)) q
    USING(a)
) t6
USING (a)
GROUP BY (t1.b)
HAVING t1.b > 200
ORDER BY 1,2;

SELECT COUNT(t1.a), t1.b FROM
ref_1 t1
LEFT JOIN
-- 2) t6 subquery is distributed so needs to be recursively planned
--    because t1 is recurring
(
    SELECT * FROM
    (SELECT * FROM ref_1 t2 JOIN dist_3_partitioned t3 USING (a) WHERE t3.b IS NULL) p
    JOIN
    -- 1) t5 is recursively planned since the outer side is recurring
    (SELECT * FROM ref_1 t4 LEFT JOIN dist_1 t5 USING (a)) q
    USING(a)
) t6
USING (a)
GROUP BY (t1.b)
HAVING (
    EXISTS (
        SELECT * FROM ref_1 t6
        LEFT JOIN dist_1 t7 USING (a)
        WHERE t7.b > 10
    )
)
ORDER BY 1,2;

SELECT COUNT(*) FROM
citus_local_1 t1
LEFT JOIN
-- 2) t6 subquery is distributed so needs to be recursively planned
--    because t1 is first recursively planned
(
    SELECT * FROM
    (SELECT * FROM ref_1 t2 JOIN dist_1 t3 USING (a) WHERE t3.b IS NULL) p
    JOIN
    -- 1) t5 is recursively planned since the outer side is recurring
    (SELECT * FROM ref_1 t4 LEFT JOIN dist_1 t5 USING (a)) q
    USING(a)
) t6
USING (a);

SELECT COUNT(*) FROM
-- 2) t1 is recursively planned because the outer side (t2) is
--    converted into a recurring rel
dist_2_columnar t1
RIGHT JOIN
(
    -- 1) t4 is recursively planned since the outer side is recurring
    ref_1 t3 LEFT JOIN dist_1 t4 USING(a)
) t2
USING (a);

SELECT COUNT(*) FROM
-- 3) t1 is recursively planned because the outer side (t2) is
--    converted into a recurring rel
dist_1 t1
RIGHT JOIN
(
    -- 2) t6 is recursively planned because now it's part of a distributed
    --    inner join node that is about to be outer joined with t3
    ref_1 t3
    LEFT JOIN
    (
        -- 1-a) t4 is recursively planned since the outer side is recurring
        (ref_1 t5 LEFT JOIN dist_1 t4 USING(a))
        JOIN
        dist_1 t6
        USING(a)
        JOIN
        -- 1-b) t8 is recursively planned since the outer side is recurring
        (ref_1 t7 LEFT JOIN dist_1 t8 USING(a))
        USING(a)
    )
    USING(a)
) t2
USING (a);

SELECT COUNT(*) FROM
ref_1 t6
LEFT JOIN
(
    ref_1 t1
    LEFT JOIN
    (
        -- t3 is a distributed join tree so needs to be recursively planned
        -- because t2 is recurring
        ref_1 t2 LEFT JOIN (dist_1 t7 JOIN dist_1 t8 USING (a)) t3 USING(a)
        JOIN
        ref_1 t5
        USING(a)
    )
    USING(a)
)
USING(a);

SELECT COUNT(*) FROM
ref_1 t6
LEFT JOIN
(
    ref_1 t1
    LEFT JOIN
    (
        -- t4 subquery is distributed so needs to be recursively planned
        -- because t2 is recurring
        ref_1 t2 LEFT JOIN (SELECT * FROM dist_1 t3) t4 USING(a)
        JOIN
        ref_1 t5
        USING(a)
    )
    USING(a)
)
USING(a);

SELECT COUNT(*) FROM
ref_1 t6
LEFT JOIN
(
    ref_1 t1
    LEFT JOIN
    (
        -- t4 subquery is distributed so needs to be recursively planned
        -- because t2 is recurring
        ref_1 t2 LEFT JOIN (SELECT * FROM dist_3_partitioned t3) t4 USING(a)
        JOIN
        ref_1 t5
        USING(a)
    )
    USING(a)
)
USING(a);

-- cannot recursively plan because t3 (inner - distributed)
-- references t1 (outer - recurring)
SELECT COUNT(*) FROM ref_1 t1 LEFT JOIN LATERAL (SELECT * FROM dist_1 t2 WHERE t1.b < t2.b) t3 USING (a);
SELECT COUNT(*) FROM (SELECT * FROM dist_1 OFFSET 100) t1 LEFT JOIN LATERAL (SELECT * FROM dist_1 t2 WHERE t1.b < t2.b) t3 USING (a);
SELECT COUNT(*) FROM local_1 t1 LEFT JOIN LATERAL (SELECT * FROM dist_1 t2 WHERE t1.b < t2.b) t3 USING (a);
SELECT COUNT(*) FROM (SELECT 1 a, generate_series(1,2) b) t1 LEFT JOIN LATERAL (SELECT * FROM dist_1 t2 WHERE t1.b < t2.b) t3 USING (a);
SELECT COUNT(*) FROM (ref_1 t10 JOIN ref_1 t11 USING(a,b)) t1 LEFT JOIN LATERAL (SELECT * FROM dist_1 t2 WHERE t1.b < t2.b) t3 USING (a);

-- cannot plan because the query in the WHERE clause of t3
-- (inner - distributed) references t1 (outer - recurring)
SELECT COUNT(*) FROM ref_1 t1
LEFT JOIN LATERAL
(
    SELECT * FROM dist_1 t2 WHERE EXISTS (
        SELECT * FROM dist_1 t4
        WHERE t4.a = t2.a AND t4.b > t1.b
    )
) t3
USING (a);

-- can recursively plan after dropping (t4.b > t1.b) qual from t3
SELECT COUNT(*) FROM ref_1 t1
LEFT JOIN
(
    SELECT * FROM dist_1 t2 WHERE EXISTS (
        SELECT * FROM dist_1 t4
        WHERE t4.a = t2.a
    )
) t3
USING (a);

-- same test using a view, can be recursively planned
CREATE VIEW my_view_1 AS
SELECT * FROM dist_1 table_name_for_view WHERE EXISTS (
    SELECT * FROM dist_1 t4
    WHERE t4.a = table_name_for_view.a);

SELECT COUNT(*) FROM ref_1 t1
LEFT JOIN
my_view_1 t3
USING (a);

SELECT COUNT(*) FROM
ref_1 t6
LEFT JOIN
(
    ref_1 t1
    LEFT JOIN
    (
        -- t4 subquery is distributed so needs to be recursively planned
        -- because t2 is recurring.
        -- However, we fail to recursively plan t4 because it references
        -- t6.
        ref_1 t2 LEFT JOIN LATERAL (SELECT * FROM dist_2_columnar t3 WHERE t3.a > t6.a) t4 USING(a)
        JOIN
        ref_1 t5
        USING(a)
    )
    USING(a)
)
USING(a);

SELECT COUNT(*) FROM
ref_1 t6
LEFT JOIN
(
    ref_1 t1
    LEFT JOIN
    (
        -- t4 subquery is distributed so needs to be recursively planned
        -- because t2 is recurring.
        -- Even if the query says t2 is lateral joined with t4, t4 doesn't
        -- reference anywhere else and hence can be planned recursively.
        ref_1 t2 LEFT JOIN LATERAL (SELECT * FROM dist_1 t3) t4 USING(a)
        JOIN
        ref_1 t5
        USING(a)
    )
    USING(a)
)
USING(a);

-- since t1 is recurring and t6 is distributed, all the distributed
-- tables in t6 will be recursively planned
SELECT COUNT(*) FROM ref_1 t1
LEFT JOIN
(
    ((SELECT * FROM ref_1 WHERE a > 1) t2 JOIN dist_1 t3 USING (a))
    JOIN
    (dist_1 t4 JOIN dist_1 t5 USING (a))
    USING(a)
) t6
USING (a);

BEGIN;
    -- same test but this time should fail due to
    -- citus.max_intermediate_result_size
    SET LOCAL citus.max_intermediate_result_size TO "0.5kB";
    SELECT COUNT(*) FROM ref_1 t1
    LEFT JOIN
    (
        ((SELECT * FROM ref_1 WHERE a > 1) t2 JOIN dist_1 t3 USING (a))
        JOIN
        (dist_1 t4 JOIN dist_1 t5 USING (a))
        USING(a)
    ) t6
    USING (a);
ROLLBACK;

-- Same test using some views, can be recursively planned too.
-- Since t1 is recurring and t6 is distributed, all the distributed
-- tables in t6 will be recursively planned.
CREATE VIEW my_view_2 AS
(SELECT * FROM ref_1 WHERE a > 1);

CREATE VIEW my_view_3 AS
(SELECT * FROM ref_1);

SELECT COUNT(*) FROM my_view_3 t1
LEFT JOIN
(
    (my_view_2 t2 JOIN dist_1 t3 USING (a))
    JOIN
    (dist_1 t4 JOIN dist_1 t5 USING (a))
    USING(a)
) t6
USING (a);

SELECT COUNT(*) FROM ref_1 t1
-- 2) Since t8 is distributed and t1 is recurring, t8 needs be converted
--    to a recurring rel too. For this reason, subquery t8 is recursively
--    planned because t7 is recurring already.
LEFT JOIN
(
    SELECT * FROM (SELECT * FROM ref_1 t2 RIGHT JOIN dist_1 t3 USING (a)) AS t4
    JOIN
    -- 1) subquery t6 is recursively planned because t5 is recurring
    (SELECT * FROM ref_1 t5 LEFT JOIN (SELECT * FROM dist_2_columnar WHERE b < 150) t6 USING (a)) as t7
    USING(a)
) t8
USING (a);

-- same test using a prepared statement
PREPARE recurring_outer_join_p1 AS
SELECT COUNT(*) FROM ref_1 t1
-- 2) Since t8 is distributed and t1 is recurring, t8 needs be converted
--    to a recurring rel too. For this reason, subquery t8 is recursively
--    planned because t7 is recurring already.
LEFT JOIN
(
    SELECT * FROM (SELECT * FROM ref_1 t2 RIGHT JOIN dist_1 t3 USING (a)) AS t4
    JOIN
    -- 1) subquery t6 is recursively planned because t5 is recurring
    (SELECT * FROM ref_1 t5 LEFT JOIN (SELECT * FROM dist_2_columnar WHERE b < $1) t6 USING (a)) as t7
    USING(a)
) t8
USING (a);

EXECUTE recurring_outer_join_p1(0);
EXECUTE recurring_outer_join_p1(100);
EXECUTE recurring_outer_join_p1(100);
EXECUTE recurring_outer_join_p1(10);
EXECUTE recurring_outer_join_p1(10);
EXECUTE recurring_outer_join_p1(1000);
EXECUTE recurring_outer_join_p1(1000);

-- t5 is recursively planned because the outer side of the final
-- left join is recurring
SELECT * FROM ref_1 t1
JOIN ref_1 t2 USING (a)
LEFT JOIN ref_1 t3 USING (a)
LEFT JOIN ref_1 t4 USING (a)
LEFT JOIN dist_1 t5 USING (a)
ORDER BY 1,2,3,4,5,6 DESC
LIMIT 5;

-- t6 is recursively planned because the outer side of the final
-- left join is recurring
SELECT * FROM (SELECT * FROM ref_1 ORDER BY 1,2 LIMIT 7) t1
JOIN ref_1 t2 USING (a)
LEFT JOIN (SELECT *, random() > 1 FROM dist_1 t3) t4 USING (a)
LEFT JOIN ref_1 t5 USING (a)
LEFT JOIN dist_1 t6 USING (a)
ORDER BY 1,2,3,4,5,6,7 DESC
LIMIT 10;

--
-- Such join rels can recursively appear anywhere in the query instead
-- of simple relation rtes.
--

SELECT COUNT(*) FROM
  (SELECT ref_1.a, t10.b FROM ref_1 LEFT JOIN dist_1 t10 USING(b)) AS t1,
  (SELECT ref_1.a, t20.b FROM ref_1 LEFT JOIN dist_1 t20 USING(b)) AS t2,
  (SELECT ref_1.a, t30.b FROM ref_1 LEFT JOIN dist_1 t30 USING(b)) AS t3,
  (SELECT ref_1.a, t40.b FROM ref_1 LEFT JOIN dist_1 t40 USING(b)) AS t4,
  (SELECT ref_1.a, t50.b FROM ref_1 LEFT JOIN dist_1 t50 USING(b)) AS t5
WHERE
  t1.a =  t5.a AND
  t1.a =  t4.a AND
  t1.a =  t3.a AND
  t1.a =  t2.a AND
  t1.a =  t1.a;

-- subqueries in the target list

SELECT t1.b, (SELECT b FROM ref_1 WHERE t1.a = a ORDER BY a,b LIMIT 1), (SELECT t2.a)
FROM ref_1
LEFT JOIN dist_1 t1 USING (a,b)
JOIN dist_1 t2 USING (a,b)
ORDER BY 1,2,3 LIMIT 5;

WITH
outer_cte_1 AS (
	SELECT
    t1.b,
    -- 9) t3 is recursively planned since t2 is recurring
    (SELECT a FROM ref_1 t2 LEFT JOIN dist_1 t3 USING(a,b) WHERE t2.a=t1.a ORDER BY 1 LIMIT 1)
	FROM dist_1 t1
	ORDER BY 1,2 LIMIT 10
),
outer_cte_2 AS (
	SELECT * FROM (
    SELECT * FROM (
      SELECT * FROM (
        SELECT * FROM (
          SELECT * FROM (
            -- 10) t5 is recursively planned since t4 is recurring
            SELECT * FROM ref_1 t4
            LEFT JOIN dist_1 t5
            USING(a,b)
          ) AS t6
        ) AS t7
      ) AS t8
    ) AS t9
    OFFSET 0
  )AS t10
  -- 11) t11 is recursively planned since lhs of the join tree became recurring
  LEFT JOIN dist_1 t11 USING (b)
)
SELECT * FROM ref_1 t36 WHERE (b,100,a) IN (
  WITH
  cte_1 AS (
    WITH cte_1_inner_cte AS (
      -- 3) t12 is recursively planned because t11 is recurring
      SELECT *  FROM ref_1 t11
      LEFT JOIN dist_1 t12
      USING (a,b)
    )
    -- 4) t14 is recursively planned because t13 is recurring
    SELECT * FROM ref_1 t13
    LEFT JOIN dist_1 t14 USING (a,b)
    JOIN cte_1_inner_cte t15
    USING (a,b)
    OFFSET 0
  )
  -- 6) t31 is recursively planned since t35 is recurring
  -- 7) t34 is recursively planned since lhs of the join tree is now recurring
  SELECT
    DISTINCT t31.b,
    -- 1) we first search for such joins in the target list and recursively plan t33
    --    because t32 is recurring
    (SELECT max(b) FROM ref_1 t32 LEFT JOIN dist_1 t33 USING(a,b) WHERE t31.a = t32.a),
    (SELECT t34.a)
  FROM ref_1 t35
  LEFT JOIN dist_1 t31 USING (a,b)
  LEFT JOIN dist_1 t34 USING (a,b)
  -- 2) cte_1 was inlided, so we then recursively check for such joins there.
  --    When doing so, we first check for cte_1_inner_cte was since it was
  --    also inlined.
  LEFT JOIN cte_1 USING (a,b)
  -- 5) Since rhs of below join is a subquery too, we recursively search
  --    for such joins there and plan distributed side of all those 10
  --    joins.
  LEFT JOIN (
    SELECT COUNT(DISTINCT t20.a) AS a
    FROM
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t20,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t21,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t22,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t23,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t24,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t25,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t26,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t27,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t28,
      (SELECT r.a, d.b FROM ref_1 r LEFT JOIN dist_1 d USING(b) WHERE r.a IS NOT NULL) AS t29
      WHERE
        t20.a = t29.a AND
        t20.a = t28.a AND
        t20.a = t27.a AND
        t20.a = t26.a AND
        t20.a = t25.a AND
        t20.a = t24.a AND
        t20.a = t23.a AND
        t20.a = t21.a AND
        t20.a = t21.a AND
        t20.a = t20.a
  ) AS t30
  ON (t30.a = cte_1.a)
  ORDER BY 1,2,3
) AND
-- 8) Then we search for such joins in the next (and final) qual of the WHERE clause.
--    Since both outer_cte_1 and outer_cte_2 were inlined, we will first
--    recursively check for such joins in them.
a NOT IN (SELECT outer_cte_1.b FROM outer_cte_1 LEFT JOIN outer_cte_2 USING (b));

WITH
cte_1 AS (
  SELECT COUNT(*) FROM dist_1 t1
  JOIN
  (
    (
      dist_1 t2 JOIN dist_1 t3 USING (a)
    )
    JOIN
    (
      dist_1 t4 JOIN (
        dist_1 t5 JOIN (
          dist_1 t6 JOIN (
            ref_1 t7 LEFT JOIN dist_1 t8 USING (a)
          ) USING(a)
        ) USING(a)
      ) USING (a)
    ) USING(a)
  ) USING (a)
),
cte_2 AS (
  SELECT COUNT(*) FROM dist_1 t9
  JOIN
  (
    (
      dist_1 t10 JOIN dist_1 t11 USING (a)
    )
    JOIN
    (
      dist_1 t12 JOIN (
        dist_1 t13 JOIN (
          dist_1 t14 JOIN (
            ref_1 t15 LEFT JOIN dist_1 t16 USING (a)
          ) USING(a)
        ) USING(a)
      ) USING (a)
    ) USING(a)
  ) USING (a)
)
SELECT * FROM cte_1, cte_2;

-- such joins can appear within SET operations too
SELECT COUNT(*) FROM
-- 2) given that the rhs of the right join is recurring due to set
--    operation, t1 is recursively planned too
dist_1 t1
RIGHT JOIN
(
    SELECT * FROM dist_1 t2
    UNION
    (
        -- 1) t3 is recursively planned because t4 is recurring
        SELECT t3.a, t3.b FROM dist_1 t3
        FULL JOIN
        ref_1 t4
        USING (a)
    )
) t5
USING(a);

-- simple modification queries

CREATE TABLE dist_5 (LIKE dist_1);
INSERT INTO dist_5 SELECT * FROM dist_1 WHERE a < 5;
SELECT create_distributed_table('dist_5', 'a');

BEGIN;
  DELETE FROM dist_5
  USING (
      SELECT t1.a, t1.b FROM ref_1 t1
      LEFT JOIN
      (
          SELECT * FROM dist_1 t2 WHERE EXISTS (
              SELECT * FROM dist_1 t4
              WHERE t4.a = t2.a
          )
      ) t3
      USING (a)
  ) q
  WHERE dist_5.a = q.a
  RETURNING *;
ROLLBACK;

BEGIN;
  UPDATE dist_5
  SET b = 10
  WHERE a IN (
    SELECT t1.a FROM ref_1 t1
    LEFT JOIN
    (
        SELECT * FROM dist_1 t2 WHERE EXISTS (
            SELECT * FROM dist_1 t4
            WHERE t4.a = t2.a
        )
    ) t3
    USING (a)
  )
  RETURNING *;
ROLLBACK;

-- INSERT .. SELECT: pull to coordinator
BEGIN;
  DELETE FROM ref_1 WHERE a IS NULL;

  INSERT INTO dist_1
  SELECT t1.*
  FROM ref_1 t1
  LEFT JOIN dist_1 t2
  ON (t1.a = t2.a);
ROLLBACK;

-- INSERT .. SELECT: repartitioned (due to <t1.a*3>)
BEGIN;
  INSERT INTO dist_1
  SELECT t1.a*3, t1.b
  FROM dist_1 t1
  JOIN
  (ref_1 t2 LEFT JOIN dist_1 t3 USING(a)) t4
  ON (t1.a = t4.a);
ROLLBACK;

-- INSERT .. SELECT: repartitioned
-- should be able to push-down once https://github.com/citusdata/citus/issues/6544 is fixed
BEGIN;
  INSERT INTO dist_1
  SELECT t1.*
  FROM dist_1 t1
  JOIN
  (ref_1 t2 LEFT JOIN dist_1 t3 USING(a)) t4
  ON (t1.a = t4.a);
ROLLBACK;

SET client_min_messages TO ERROR;
DROP SCHEMA recurring_outer_join CASCADE;
