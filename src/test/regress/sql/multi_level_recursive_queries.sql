-- multi recursive queries with joins, subqueries, and ctes
CREATE SCHEMA multi_recursive;
SET search_path TO multi_recursive;


DROP TABLE IF EXISTS tbl_dist1;
CREATE TABLE tbl_dist1(id int);
SELECT create_distributed_table('tbl_dist1','id');

DROP TABLE IF EXISTS tbl_ref1;
CREATE TABLE tbl_ref1(id int);
SELECT create_reference_table('tbl_ref1');

INSERT INTO tbl_dist1 SELECT i FROM generate_series(0,10) i;
INSERT INTO tbl_ref1 SELECT i FROM generate_series(0,10) i;

-- https://github.com/citusdata/citus/issues/6653
-- The reason why inlined queries failed are all the same. After we modified the query at first pass, second pass finds out
-- noncolocated queries as we donot create equivalances between nondistributed-distributed tables.

-- QUERY1
-- recursive planner multipass the query and fails.
-- Why inlined query failed?
--  limit clause is recursively planned in inlined cte. First pass finishes here. At second pass, noncolocated queries and
--  recurring full join are recursively planned. We detect that and throw error.
SELECT t1.id
FROM (
    SELECT t2.id
    FROM (
        SELECT t0.id
        FROM tbl_dist1 t0
        LIMIT 5
    ) AS t2
    INNER JOIN tbl_dist1 AS t3 USING (id)
) AS t1
FULL JOIN tbl_dist1 t4 USING (id);

-- QUERY2
-- recursive planner multipass the query with inlined cte and fails. Then, cte is planned without inlining and it succeeds.
-- Why inlined query failed?
--  recurring left join is recursively planned in inlined cte. Then, limit clause causes another recursive planning. First pass
--  finishes here. At second pass, noncolocated queries and recurring right join are recursively planned. We detect that and
--  throw error.
SET client_min_messages TO DEBUG1;
WITH cte_0 AS (
    SELECT id FROM tbl_dist1 WHERE id IN (
        SELECT id FROM tbl_ref1
        LEFT JOIN tbl_dist1 USING (id)
    )
)
SELECT count(id) FROM tbl_dist1
RIGHT JOIN (
    SELECT table_5.id FROM (
        SELECT id FROM cte_0 LIMIT 0
    ) AS table_5
    RIGHT JOIN tbl_dist1 USING (id)
) AS table_4 USING (id);
RESET client_min_messages;

DROP TABLE IF EXISTS dist0;
CREATE TABLE dist0(id int);
SELECT create_distributed_table('dist0','id');

DROP TABLE IF EXISTS dist1;
CREATE TABLE dist1(id int);
SELECT create_distributed_table('dist1','id');

INSERT INTO dist0 SELECT i FROM generate_series(1005,1025) i;
INSERT INTO dist1 SELECT i FROM generate_series(1015,1035) i;

-- QUERY3
-- recursive planner multipass the query with inlined cte and fails. Then, cte is planned without inlining and it succeeds.
-- Why inlined query failed?
--  noncolocated queries are recursively planned. First pass finishes here. Second pass also recursively plans noncolocated
--  queries and recurring full join. We detect the error and throw it.
SET client_min_messages TO DEBUG1;
WITH cte_0 AS (
    SELECT id FROM dist0
    RIGHT JOIN dist0 AS table_1 USING (id)
    ORDER BY id
)
SELECT avg(avgsub.id) FROM (
    SELECT table_2.id FROM (
        SELECT table_3.id FROM (
            SELECT table_5.id FROM cte_0 AS table_5, dist1
        ) AS table_3 INNER JOIN dist1 USING (id)
    ) AS table_2 FULL JOIN dist0 USING (id)
) AS avgsub;
RESET client_min_messages;

DROP TABLE IF EXISTS dist0;
CREATE TABLE dist0(id int);
SELECT create_distributed_table('dist0','id');

DROP TABLE IF EXISTS dist1;
CREATE TABLE dist1(id int);
SELECT create_distributed_table('dist1','id');

INSERT INTO dist0 SELECT i FROM generate_series(0,10) i;
INSERT INTO dist0 SELECT * FROM dist0 ORDER BY id LIMIT 1;

INSERT INTO dist1 SELECT i FROM generate_series(0,10) i;
INSERT INTO dist1 SELECT * FROM dist1 ORDER BY id LIMIT 1;

-- QUERY4
-- recursive planner multipass the query fails.
-- Why inlined query failed?
--  limit clause is recursively planned at the first pass. At second pass noncolocated queries are recursively planned.
--  We detect that and throw error.
SET client_min_messages TO DEBUG1;
SELECT avg(avgsub.id) FROM (
    SELECT table_0.id FROM (
        SELECT table_1.id FROM (
            SELECT table_2.id FROM (
                SELECT table_3.id FROM (
                    SELECT table_4.id FROM dist0 AS table_4
                    LEFT JOIN dist1 AS table_5 USING (id)
                ) AS table_3 INNER JOIN dist0 AS table_6 USING (id)
            ) AS table_2 WHERE table_2.id < 10 ORDER BY id LIMIT 47
        ) AS table_1 RIGHT JOIN dist0 AS table_7 USING (id)
    ) AS table_0 RIGHT JOIN dist1 AS table_8 USING (id)
) AS avgsub;

-- QUERY5
-- recursive planner multipass the query with inlined cte and fails. Then, cte is planned without inlining and it succeeds.
-- Why inlined query failed?
--  limit clause is recursively planned. First pass finishes here. At second pass, noncolocated tables and recurring full join
--  are recursively planned. We detect that and throw error.
WITH cte_0 AS (
    SELECT table_0.id FROM dist1 AS table_0 LEFT JOIN dist1 AS table_1 USING (id) ORDER BY id LIMIT 41
)
SELECT avg(avgsub.id) FROM (
    SELECT table_4.id FROM (
        SELECT table_5.id FROM (
            SELECT table_6.id FROM cte_0 AS table_6
        ) AS table_5
        INNER JOIN dist0 USING (id) INNER JOIN dist1 AS table_9 USING (id)
    ) AS table_4 FULL JOIN dist0 USING (id)
) AS avgsub;

-- QUERY6
-- recursive planner multipass the query with inlined cte and fails. Then, cte is planned without inlining and it succeeds.
-- Why inlined query failed?
--  Same query and flow as above with explicit (NOT MATERIALIZED) option, which makes it directly inlinable. Even if
--  planner fails with inlined query, it succeeds without inlining.
WITH cte_0 AS (
    SELECT table_0.id FROM dist1 AS table_0 LEFT JOIN dist1 AS table_1 USING (id) ORDER BY id LIMIT 41
)
SELECT avg(avgsub.id) FROM (
    SELECT table_4.id FROM (
        SELECT table_5.id FROM (
            SELECT table_6.id FROM cte_0 AS table_6
        ) AS table_5
        INNER JOIN dist0 USING (id) INNER JOIN dist1 AS table_9 USING (id)
    ) AS table_4 FULL JOIN dist0 USING (id)
) AS avgsub;

-- QUERY7
-- recursive planner multipass the query and fails. Note that cte is not used in the query.
-- Why inlined query failed?
--  limit clause is recursively planned. First pass finishes here. At second pass noncolocated queries are recursively planned.
--  We detect multipass and throw error.
WITH cte_0 AS (
    SELECT table_0.id FROM dist1 AS table_0 FULL JOIN dist1 AS table_1 USING (id)
)
SELECT avg(table_5.id) FROM (
    SELECT table_6.id FROM (
        SELECT table_7.id FROM dist0 AS table_7 ORDER BY id LIMIT 87
    ) AS table_6 INNER JOIN dist0 AS table_8 USING (id) WHERE table_8.id < 0 ORDER BY id
) AS table_5 INNER JOIN dist0 AS table_9 USING (id);


SET client_min_messages TO WARNING;
DROP SCHEMA multi_recursive CASCADE;
