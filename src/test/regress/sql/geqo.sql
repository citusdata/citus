-- test geqo
CREATE SCHEMA geqo_schema;
SET search_path TO geqo_schema;

CREATE TABLE dist (a int, b int);
SELECT create_distributed_table('dist', 'a');
INSERT INTO dist VALUES (1, 1), (2, 2), (3, 3);

CREATE TABLE dist2 (a int, b int);
SELECT create_distributed_table('dist2', 'a');
INSERT INTO dist2 VALUES (1, 1), (2, 2), (3, 3);

SET geqo_threshold TO 2;
SET geqo_pool_size TO 1000;
SET geqo_generations TO 1000;

SET citus.enable_repartition_joins to ON;

SELECT count(*) FROM dist d1 LEFT JOIN dist2 d2 ON d1.a = d2.a LEFT JOIN dist d3 ON d3.a = d2.a;

-- JOINs  with CTEs:
WITH cte_1 AS (SELECT * FROM dist OFFSET 0)
	SELECT count(*) FROM dist d1 LEFT JOIN dist2 d2 ON d1.a = d2.a LEFT JOIN dist d3 ON d3.a = d2.a LEFT JOIN cte_1 ON true;

WITH cte_1 AS (SELECT * FROM dist OFFSET 0),
	 cte_2 AS (SELECT * FROM dist2 OFFSET 0),
	 cte_3 AS (SELECT * FROM dist OFFSET 0)
	SELECT count(*) FROM cte_1 d1 LEFT JOIN cte_2 d2 ON d1.a = d2.a LEFT JOIN cte_3 d3 ON d3.a = d2.a LEFT JOIN cte_1 ON true;

-- Inner JOIN:
SELECT count(*) FROM dist d1 JOIN dist2 d2 ON d1.a = d2.a JOIN dist d3 ON d3.a = d2.a;

-- subquery join
SELECT count(*) FROM (SELECT *, random() FROM dist) as d1 JOIN (SELECT *, random() FROM  dist2) d2 ON d1.a = d2.a JOIN (SELECT *, random() FROM dist) as d3 ON d3.a = d2.a;
SELECT count(*) FROM (SELECT *, random() FROM dist) as d1 LEFT JOIN (SELECT *, random() FROM  dist2) d2 ON d1.a = d2.a LEFT JOIN (SELECT *, random() FROM dist) as d3 ON d3.a = d2.a;

-- router query
SELECT count(*) FROM dist d1 LEFT JOIN dist2 d2 ON d1.a = d2.a LEFT JOIN dist d3 ON d3.a = d2.a WHERE d1.a = 1 AND d2.a = 1 AND d3.a = 1;

-- fast path router query
SELECT count(*) FROM dist WHERE a = 1;

-- simple INSERT
INSERT INTO dist (a) VALUES (1);

-- repartition join, probably not relevant, but still be defensive
SELECT count(*) FROM dist d1 JOIN dist2 d2 ON d1.b = d2.b JOIN dist d3 ON d3.b = d2.b;

-- update query with join
UPDATE dist SET b = foo.a FROM (SELECT d1.a FROM dist d1 JOIN dist2 d2 USING(a)) foo WHERE  foo.a = dist.a RETURNING *;

-- insert select via repartitioning
INSERT INTO dist (a) SELECT max(d1.b) FROM dist d1 JOIN dist2 d2 ON d1.a = d2.a JOIN dist d3 ON d3.a = d2.a GROUP BY d1.a;
SELECT count(*) FROM dist;

-- insert select pushdown
INSERT INTO dist SELECT d1.* FROM dist d1 JOIN dist2 d2 ON d1.a = d2.a JOIN dist d3 ON d3.a = d2.a WHERE d1.b < 2 AND d2.b < 2;
SELECT count(*) FROM dist;

-- insert select via coordinator
INSERT INTO dist SELECT d1.* FROM dist d1 JOIN dist2 d2 ON d1.a = d2.a JOIN dist d3 ON d3.a = d2.a WHERE d1.b < 2 AND d2.b < 2 OFFSET 0;
SELECT count(*) FROM dist;

DROP SCHEMA geqo_schema CASCADE;
