

-- ===================================================================
-- test router planner functionality for single shard select queries
-- ===================================================================

-- run all the router queries from the one of the workers

\c - - - :worker_1_port
-- this table is used in a CTE test
CREATE TABLE authors_hash_mx ( name text, id bigint );

-- create a bunch of test data
INSERT INTO articles_hash_mx VALUES ( 1,  1, 'arsenous', 9572);
INSERT INTO articles_hash_mx VALUES ( 2,  2, 'abducing', 13642);
INSERT INTO articles_hash_mx VALUES ( 3,  3, 'asternal', 10480);
INSERT INTO articles_hash_mx VALUES ( 4,  4, 'altdorfer', 14551);
INSERT INTO articles_hash_mx VALUES ( 5,  5, 'aruru', 11389);
INSERT INTO articles_hash_mx VALUES ( 6,  6, 'atlases', 15459);
INSERT INTO articles_hash_mx VALUES ( 7,  7, 'aseptic', 12298);
INSERT INTO articles_hash_mx VALUES ( 8,  8, 'agatized', 16368);
INSERT INTO articles_hash_mx VALUES ( 9,  9, 'alligate', 438);
INSERT INTO articles_hash_mx VALUES (10, 10, 'aggrandize', 17277);
INSERT INTO articles_hash_mx VALUES (11,  1, 'alamo', 1347);
INSERT INTO articles_hash_mx VALUES (12,  2, 'archiblast', 18185);
INSERT INTO articles_hash_mx VALUES (13,  3, 'aseyev', 2255);
INSERT INTO articles_hash_mx VALUES (14,  4, 'andesite', 19094);
INSERT INTO articles_hash_mx VALUES (15,  5, 'adversa', 3164);
INSERT INTO articles_hash_mx VALUES (16,  6, 'allonym', 2);
INSERT INTO articles_hash_mx VALUES (17,  7, 'auriga', 4073);
INSERT INTO articles_hash_mx VALUES (18,  8, 'assembly', 911);
INSERT INTO articles_hash_mx VALUES (19,  9, 'aubergiste', 4981);
INSERT INTO articles_hash_mx VALUES (20, 10, 'absentness', 1820);
INSERT INTO articles_hash_mx VALUES (21,  1, 'arcading', 5890);
INSERT INTO articles_hash_mx VALUES (22,  2, 'antipope', 2728);
INSERT INTO articles_hash_mx VALUES (23,  3, 'abhorring', 6799);
INSERT INTO articles_hash_mx VALUES (24,  4, 'audacious', 3637);
INSERT INTO articles_hash_mx VALUES (25,  5, 'antehall', 7707);
INSERT INTO articles_hash_mx VALUES (26,  6, 'abington', 4545);
INSERT INTO articles_hash_mx VALUES (27,  7, 'arsenous', 8616);
INSERT INTO articles_hash_mx VALUES (28,  8, 'aerophyte', 5454);
INSERT INTO articles_hash_mx VALUES (29,  9, 'amateur', 9524);
INSERT INTO articles_hash_mx VALUES (30, 10, 'andelee', 6363);
INSERT INTO articles_hash_mx VALUES (31,  1, 'athwartships', 7271);
INSERT INTO articles_hash_mx VALUES (32,  2, 'amazon', 11342);
INSERT INTO articles_hash_mx VALUES (33,  3, 'autochrome', 8180);
INSERT INTO articles_hash_mx VALUES (34,  4, 'amnestied', 12250);
INSERT INTO articles_hash_mx VALUES (35,  5, 'aminate', 9089);
INSERT INTO articles_hash_mx VALUES (36,  6, 'ablation', 13159);
INSERT INTO articles_hash_mx VALUES (37,  7, 'archduchies', 9997);
INSERT INTO articles_hash_mx VALUES (38,  8, 'anatine', 14067);
INSERT INTO articles_hash_mx VALUES (39,  9, 'anchises', 10906);
INSERT INTO articles_hash_mx VALUES (40, 10, 'attemper', 14976);
INSERT INTO articles_hash_mx VALUES (41,  1, 'aznavour', 11814);
INSERT INTO articles_hash_mx VALUES (42,  2, 'ausable', 15885);
INSERT INTO articles_hash_mx VALUES (43,  3, 'affixal', 12723);
INSERT INTO articles_hash_mx VALUES (44,  4, 'anteport', 16793);
INSERT INTO articles_hash_mx VALUES (45,  5, 'afrasia', 864);
INSERT INTO articles_hash_mx VALUES (46,  6, 'atlanta', 17702);
INSERT INTO articles_hash_mx VALUES (47,  7, 'abeyance', 1772);
INSERT INTO articles_hash_mx VALUES (48,  8, 'alkylic', 18610);
INSERT INTO articles_hash_mx VALUES (49,  9, 'anyone', 2681);
INSERT INTO articles_hash_mx VALUES (50, 10, 'anjanette', 19519);



SET citus.task_executor_type TO 'real-time';
SET citus.large_table_shard_count TO 2;
SET client_min_messages TO 'DEBUG2';

-- insert a single row for the test
INSERT INTO articles_single_shard_hash_mx VALUES (50, 10, 'anjanette', 19519);

-- single-shard tests

-- test simple select for a single row
SELECT * FROM articles_hash_mx WHERE author_id = 10 AND id = 50;

-- get all titles by a single author
SELECT title FROM articles_hash_mx WHERE author_id = 10;

-- try ordering them by word count
SELECT title, word_count FROM articles_hash_mx
	WHERE author_id = 10
	ORDER BY word_count DESC NULLS LAST;

-- look at last two articles by an author
SELECT title, id FROM articles_hash_mx
	WHERE author_id = 5
	ORDER BY id
	LIMIT 2;

-- find all articles by two authors in same shard
-- but plan is not router executable due to order by
SELECT title, author_id FROM articles_hash_mx
	WHERE author_id = 7 OR author_id = 8
	ORDER BY author_id ASC, id;

-- same query is router executable with no order by
SELECT title, author_id FROM articles_hash_mx
	WHERE author_id = 7 OR author_id = 8;

-- add in some grouping expressions, still on same shard
-- having queries unsupported in Citus
SELECT author_id, sum(word_count) AS corpus_size FROM articles_hash_mx
	WHERE author_id = 1 OR author_id = 7 OR author_id = 8 OR author_id = 10
	GROUP BY author_id
	HAVING sum(word_count) > 1000
	ORDER BY sum(word_count) DESC;

-- however having clause is supported if it goes to a single shard
SELECT author_id, sum(word_count) AS corpus_size FROM articles_hash_mx
	WHERE author_id = 1
	GROUP BY author_id
	HAVING sum(word_count) > 1000
	ORDER BY sum(word_count) DESC;

-- query is a single shard query but can't do shard pruning,
-- not router-plannable due to <= and IN
SELECT * FROM articles_hash_mx WHERE author_id <= 1; 
SELECT * FROM articles_hash_mx WHERE author_id IN (1, 3); 

-- queries with CTEs are supported
WITH first_author AS ( SELECT id FROM articles_hash_mx WHERE author_id = 1)
SELECT * FROM first_author;

-- queries with CTEs are supported even if CTE is not referenced inside query
WITH first_author AS ( SELECT id FROM articles_hash_mx WHERE author_id = 1)
SELECT title FROM articles_hash_mx WHERE author_id = 1;

-- two CTE joins are supported if they go to the same worker
WITH id_author AS ( SELECT id, author_id FROM articles_hash_mx WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash_mx WHERE author_id = 1)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

WITH id_author AS ( SELECT id, author_id FROM articles_hash_mx WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash_mx WHERE author_id = 3)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- CTE joins on different workers are supported because they are both planned recursively
WITH id_author AS ( SELECT id, author_id FROM articles_hash_mx WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash_mx WHERE author_id = 2)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- recursive CTEs are supported when filtered on partition column

INSERT INTO company_employees_mx values(1, 1, 0);
INSERT INTO company_employees_mx values(1, 2, 1);
INSERT INTO company_employees_mx values(1, 3, 1);
INSERT INTO company_employees_mx values(1, 4, 2);
INSERT INTO company_employees_mx values(1, 5, 4);

INSERT INTO company_employees_mx values(3, 1, 0);
INSERT INTO company_employees_mx values(3, 15, 1);
INSERT INTO company_employees_mx values(3, 3, 1);

-- find employees at top 2 level within company hierarchy
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees_mx
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees_mx ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 1))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- query becomes not router plannble and gets rejected
-- if filter on company is dropped
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees_mx
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees_mx ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- logically wrong query, query involves different shards
-- from the same table, but still router plannable due to
-- shard being placed on the same worker.
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees_mx
		WHERE company_id = 3 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees_mx ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 2))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- grouping sets are supported on single shard
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash_mx
	WHERE author_id = 1 or author_id = 3
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- grouping sets are not supported on multiple shards
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash_mx
	WHERE author_id = 1 or author_id = 2
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- queries which involve functions in FROM clause are supported if it goes to a single worker.
SELECT * FROM articles_hash_mx, position('om' in 'Thomas') WHERE author_id = 1;

SELECT * FROM articles_hash_mx, position('om' in 'Thomas') WHERE author_id = 1 or author_id = 3;

-- they are supported via (sub)query pushdown if multiple workers are involved
SELECT * FROM articles_hash_mx, position('om' in 'Thomas') WHERE author_id = 1 or author_id = 2 ORDER BY 4 DESC, 1 DESC, 2 DESC LIMIT 5;

-- subqueries are supported in FROM clause but they are not router plannable
SELECT articles_hash_mx.id,test.word_count
FROM articles_hash_mx, (SELECT id, word_count FROM articles_hash_mx) AS test WHERE test.id = articles_hash_mx.id
ORDER BY articles_hash_mx.id;


SELECT articles_hash_mx.id,test.word_count
FROM articles_hash_mx, (SELECT id, word_count FROM articles_hash_mx) AS test 
WHERE test.id = articles_hash_mx.id and articles_hash_mx.author_id = 1
ORDER BY articles_hash_mx.id;

-- subqueries are not supported in SELECT clause
SELECT a.title AS name, (SELECT a2.id FROM articles_single_shard_hash_mx a2 WHERE a.id = a2.id  LIMIT 1)
						 AS special_price FROM articles_hash_mx a;

-- simple lookup query
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1;

-- below query hits a single shard, router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 OR author_id = 17;

-- below query hits two shards, not router plannable + not router executable
-- handled by real-time executor
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 OR author_id = 18;

-- rename the output columns
SELECT id as article_id, word_count * id as random_value
	FROM articles_hash_mx
	WHERE author_id = 1;

-- we can push down co-located joins to a single worker
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash_mx a, articles_hash_mx b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- following join is router plannable since the same worker 
-- has both shards
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash_mx a, articles_single_shard_hash_mx b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;
	
-- following join is not router plannable since there are no
-- workers containing both shards, but will work through recursive
-- planning
WITH single_shard as (SELECT * FROM articles_single_shard_hash_mx)
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash_mx a, single_shard b
	WHERE a.author_id = 2 and a.author_id = b.author_id
	LIMIT 3;

-- single shard select with limit is router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	LIMIT 3;

-- single shard select with limit + offset is router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	LIMIT 2
	OFFSET 1;

-- single shard select with limit + offset + order by is router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	ORDER BY id desc
	LIMIT 2
	OFFSET 1;
	
-- single shard select with group by on non-partition column is router plannable
SELECT id
	FROM articles_hash_mx
	WHERE author_id = 1
	GROUP BY id
	ORDER BY id;

-- single shard select with distinct is router plannable
SELECT distinct id
	FROM articles_hash_mx
	WHERE author_id = 1
	ORDER BY id;

-- single shard aggregate is router plannable
SELECT avg(word_count)
	FROM articles_hash_mx
	WHERE author_id = 2;

-- max, min, sum, count are router plannable on single shard
SELECT max(word_count) as max, min(word_count) as min,
	   sum(word_count) as sum, count(word_count) as cnt
	FROM articles_hash_mx
	WHERE author_id = 2;


-- queries with aggregates and group by supported on single shard
SELECT max(word_count)
	FROM articles_hash_mx
	WHERE author_id = 1
	GROUP BY author_id;

	
-- router plannable union queries are supported
SELECT * FROM (
	SELECT * FROM articles_hash_mx WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash_mx WHERE author_id = 3
) AS combination
ORDER BY id;

(SELECT LEFT(title, 1) FROM articles_hash_mx WHERE author_id = 1)
UNION
(SELECT LEFT(title, 1) FROM articles_hash_mx WHERE author_id = 3);

(SELECT LEFT(title, 1) FROM articles_hash_mx WHERE author_id = 1)
INTERSECT
(SELECT LEFT(title, 1) FROM articles_hash_mx WHERE author_id = 3);

SELECT * FROM (
	SELECT LEFT(title, 2) FROM articles_hash_mx WHERE author_id = 1
	EXCEPT
	SELECT LEFT(title, 2) FROM articles_hash_mx WHERE author_id = 3
) AS combination
ORDER BY 1;

-- union queries are not supported if not router plannable
-- there is an inconsistency on shard pruning between
-- ubuntu/mac disabling log messages for this queries only

SET client_min_messages to 'NOTICE';

(SELECT * FROM articles_hash_mx WHERE author_id = 1)
UNION
(SELECT * FROM articles_hash_mx WHERE author_id = 2);


SELECT * FROM (
	(SELECT * FROM articles_hash_mx WHERE author_id = 1)
	UNION
	(SELECT * FROM articles_hash_mx WHERE author_id = 2)) uu
ORDER BY 1, 2
LIMIT 5;

-- error out for queries with repartition jobs
SELECT *
	FROM articles_hash_mx a, articles_hash_mx b
	WHERE a.id = b.id  AND a.author_id = 1;

-- queries which hit more than 1 shards are not router plannable or executable
-- handled by real-time executor
SELECT *
	FROM articles_hash_mx
	WHERE author_id >= 1 AND author_id <= 3;

SET citus.task_executor_type TO 'real-time';

-- Test various filtering options for router plannable check
SET client_min_messages to 'DEBUG2';

-- this is definitely single shard
-- and router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 and author_id >= 1;

-- not router plannable due to or
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 or id = 1;
	
-- router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 and (id = 1 or id = 41);

-- router plannable
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 and (id = random()::int  * 0);

-- not router plannable due to function call on the right side
SELECT *
	FROM articles_hash_mx
	WHERE author_id = (random()::int  * 0 + 1);

-- not router plannable due to or
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 or id = 1;
	
-- router plannable due to abs(-1) getting converted to 1 by postgresql
SELECT *
	FROM articles_hash_mx
	WHERE author_id = abs(-1);

-- not router plannable due to abs() function
SELECT *
	FROM articles_hash_mx
	WHERE 1 = abs(author_id);

-- not router plannable due to abs() function
SELECT *
	FROM articles_hash_mx
	WHERE author_id = abs(author_id - 2);

-- router plannable, function on different field
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1 and (id = abs(id - 2));

-- not router plannable due to is true
SELECT *
	FROM articles_hash_mx
	WHERE (author_id = 1) is true;

-- router plannable, (boolean expression) = true is collapsed to (boolean expression)
SELECT *
	FROM articles_hash_mx
	WHERE (author_id = 1) = true;

-- router plannable, between operator is on another column
SELECT *
	FROM articles_hash_mx
	WHERE (author_id = 1) and id between 0 and 20;

-- router plannable, partition column expression is and'ed to rest
SELECT *
	FROM articles_hash_mx
	WHERE (author_id = 1) and (id = 1 or id = 31) and title like '%s';

-- router plannable, order is changed
SELECT *
	FROM articles_hash_mx
	WHERE (id = 1 or id = 31) and title like '%s' and (author_id = 1);

-- router plannable
SELECT *
	FROM articles_hash_mx
	WHERE (title like '%s' or title like 'a%') and (author_id = 1);

-- router plannable
SELECT *
	FROM articles_hash_mx
	WHERE (title like '%s' or title like 'a%') and (author_id = 1) and (word_count < 3000 or word_count > 8000);

-- window functions are supported if query is router plannable
SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash_mx
	WHERE author_id = 5;

SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash_mx
	WHERE author_id = 5
	ORDER BY word_count DESC;

SELECT id, MIN(id) over (order by word_count)
	FROM articles_hash_mx
	WHERE author_id = 1;

SELECT id, word_count, AVG(word_count) over (order by word_count)
	FROM articles_hash_mx
	WHERE author_id = 1;

SELECT word_count, rank() OVER (PARTITION BY author_id ORDER BY word_count)  
	FROM articles_hash_mx 
	WHERE author_id = 1;

-- window functions are not supported for not router plannable queries
SELECT id, MIN(id) over (order by word_count)
	FROM articles_hash_mx
	WHERE author_id = 1 or author_id = 2;

SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash_mx
	WHERE author_id = 5 or author_id = 2;

-- complex query hitting a single shard 	
SELECT
	count(DISTINCT CASE
			WHEN 
				word_count > 100
			THEN
				id
			ELSE
				NULL
			END) as c
	FROM
		articles_hash_mx
	WHERE
		author_id = 5;

-- same query is not router plannable if hits multiple shards
SELECT
	count(DISTINCT CASE
			WHEN 
				word_count > 100
			THEN
				id
			ELSE
				NULL
			END) as c
	FROM
		articles_hash_mx
 	GROUP BY
		author_id;

-- queries inside transactions can be router plannable
BEGIN;
SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	ORDER BY id;
END;

-- cursor queries are router plannable
BEGIN;
DECLARE test_cursor CURSOR FOR 
	SELECT *
		FROM articles_hash_mx
		WHERE author_id = 1
		ORDER BY id;
FETCH test_cursor;
FETCH test_cursor;
FETCH BACKWARD test_cursor;
END;

-- queries inside copy can be router plannable
COPY (
	SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	ORDER BY id) TO STDOUT;
	
-- table creation queries inside can be router plannable
CREATE TEMP TABLE temp_articles_hash_mx as
	SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1
	ORDER BY id;

-- router plannable queries may include filter for aggragates
SELECT count(*), count(*) FILTER (WHERE id < 3)
	FROM articles_hash_mx
	WHERE author_id = 1;

-- non-router plannable queries support filters as well
SELECT count(*), count(*) FILTER (WHERE id < 3)
	FROM articles_hash_mx
	WHERE author_id = 1 or author_id = 2;

-- prepare queries can be router plannable
PREPARE author_1_articles as
	SELECT *
	FROM articles_hash_mx
	WHERE author_id = 1;

EXECUTE author_1_articles;

-- parametric prepare queries can be router plannable
PREPARE author_articles(int) as
	SELECT *
	FROM articles_hash_mx
	WHERE author_id = $1;

EXECUTE author_articles(1);

-- queries inside plpgsql functions could be router plannable
CREATE OR REPLACE FUNCTION author_articles_max_id() RETURNS int AS $$
DECLARE
  max_id integer;
BEGIN
	SELECT MAX(id) FROM articles_hash_mx ah
		WHERE author_id = 1
		into max_id;
	return max_id;
END;
$$ LANGUAGE plpgsql;

SELECT author_articles_max_id();

-- plpgsql function that return query results are not router plannable
CREATE OR REPLACE FUNCTION author_articles_id_word_count() RETURNS TABLE(id bigint, word_count int) AS $$
DECLARE
BEGIN
	RETURN QUERY
		SELECT ah.id, ah.word_count
		FROM articles_hash_mx ah
		WHERE author_id = 1;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM author_articles_id_word_count();

-- materialized views can be created for router plannable queries
CREATE MATERIALIZED VIEW mv_articles_hash_mx AS
	SELECT * FROM articles_hash_mx WHERE author_id = 1;

SELECT * FROM mv_articles_hash_mx;

SET client_min_messages to 'INFO';
DROP MATERIALIZED VIEW mv_articles_hash_mx;
SET client_min_messages to 'DEBUG2';

CREATE MATERIALIZED VIEW mv_articles_hash_mx_error AS
	SELECT * FROM articles_hash_mx WHERE author_id in (1,2);
	
-- router planner/executor is disabled for task-tracker executor
-- following query is router plannable, but router planner is disabled

-- TODO: Uncomment once we fix task-tracker issue
--SET citus.task_executor_type to 'task-tracker';
--SELECT id
--	FROM articles_hash_mx
--	WHERE author_id = 1;

-- insert query is router plannable even under task-tracker
INSERT INTO articles_hash_mx VALUES (51,  1, 'amateus', 1814);

-- verify insert is successfull (not router plannable and executable)
SELECT id
	FROM articles_hash_mx
	WHERE author_id = 1;
