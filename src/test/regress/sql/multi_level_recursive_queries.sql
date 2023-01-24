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

-- QUERY1
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

-- QUERY2
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

-- QUERY3
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

-- QUERY4
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

-- QUERY5
WITH cte_0 AS (
    SELECT table_0.id FROM dist1 AS table_0 FULL JOIN dist1 AS table_1 USING (id)
)
SELECT avg(table_5.id) FROM (
    SELECT table_6.id FROM (
        SELECT table_7.id FROM dist0 AS table_7 ORDER BY id LIMIT 87
    ) AS table_6 INNER JOIN dist0 AS table_8 USING (id) WHERE table_8.id < 0 ORDER BY id
) AS table_5 INNER JOIN dist0 AS table_9 USING (id);


RESET client_min_messages;
DROP SCHEMA multi_recursive CASCADE;
