
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 840000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 840000;


-- ===================================================================
-- test router planner functionality for single shard select queries
-- ===================================================================

CREATE TABLE articles_hash (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer
);

-- Check for the existence of line 'DEBUG:  Creating router plan'
-- to determine if router planner is used.

-- this table is used in a CTE test
CREATE TABLE authors_hash ( name text, id bigint );

-- this table is used in router executor tests
CREATE TABLE articles_single_shard_hash (LIKE articles_hash);

SELECT master_create_distributed_table('articles_hash', 'author_id', 'hash');
SELECT master_create_distributed_table('articles_single_shard_hash', 'author_id', 'hash');


-- test when a table is distributed but no shards created yet
SELECT count(*) from articles_hash;

SELECT master_create_worker_shards('articles_hash', 2, 1);
SELECT master_create_worker_shards('articles_single_shard_hash', 1, 1);

-- create a bunch of test data
INSERT INTO articles_hash VALUES ( 1,  1, 'arsenous', 9572);
INSERT INTO articles_hash VALUES ( 2,  2, 'abducing', 13642);
INSERT INTO articles_hash VALUES ( 3,  3, 'asternal', 10480);
INSERT INTO articles_hash VALUES ( 4,  4, 'altdorfer', 14551);
INSERT INTO articles_hash VALUES ( 5,  5, 'aruru', 11389);
INSERT INTO articles_hash VALUES ( 6,  6, 'atlases', 15459);
INSERT INTO articles_hash VALUES ( 7,  7, 'aseptic', 12298);
INSERT INTO articles_hash VALUES ( 8,  8, 'agatized', 16368);
INSERT INTO articles_hash VALUES ( 9,  9, 'alligate', 438);
INSERT INTO articles_hash VALUES (10, 10, 'aggrandize', 17277);
INSERT INTO articles_hash VALUES (11,  1, 'alamo', 1347);
INSERT INTO articles_hash VALUES (12,  2, 'archiblast', 18185);
INSERT INTO articles_hash VALUES (13,  3, 'aseyev', 2255);
INSERT INTO articles_hash VALUES (14,  4, 'andesite', 19094);
INSERT INTO articles_hash VALUES (15,  5, 'adversa', 3164);
INSERT INTO articles_hash VALUES (16,  6, 'allonym', 2);
INSERT INTO articles_hash VALUES (17,  7, 'auriga', 4073);
INSERT INTO articles_hash VALUES (18,  8, 'assembly', 911);
INSERT INTO articles_hash VALUES (19,  9, 'aubergiste', 4981);
INSERT INTO articles_hash VALUES (20, 10, 'absentness', 1820);
INSERT INTO articles_hash VALUES (21,  1, 'arcading', 5890);
INSERT INTO articles_hash VALUES (22,  2, 'antipope', 2728);
INSERT INTO articles_hash VALUES (23,  3, 'abhorring', 6799);
INSERT INTO articles_hash VALUES (24,  4, 'audacious', 3637);
INSERT INTO articles_hash VALUES (25,  5, 'antehall', 7707);
INSERT INTO articles_hash VALUES (26,  6, 'abington', 4545);
INSERT INTO articles_hash VALUES (27,  7, 'arsenous', 8616);
INSERT INTO articles_hash VALUES (28,  8, 'aerophyte', 5454);
INSERT INTO articles_hash VALUES (29,  9, 'amateur', 9524);
INSERT INTO articles_hash VALUES (30, 10, 'andelee', 6363);
INSERT INTO articles_hash VALUES (31,  1, 'athwartships', 7271);
INSERT INTO articles_hash VALUES (32,  2, 'amazon', 11342);
INSERT INTO articles_hash VALUES (33,  3, 'autochrome', 8180);
INSERT INTO articles_hash VALUES (34,  4, 'amnestied', 12250);
INSERT INTO articles_hash VALUES (35,  5, 'aminate', 9089);
INSERT INTO articles_hash VALUES (36,  6, 'ablation', 13159);
INSERT INTO articles_hash VALUES (37,  7, 'archduchies', 9997);
INSERT INTO articles_hash VALUES (38,  8, 'anatine', 14067);
INSERT INTO articles_hash VALUES (39,  9, 'anchises', 10906);
INSERT INTO articles_hash VALUES (40, 10, 'attemper', 14976);
INSERT INTO articles_hash VALUES (41,  1, 'aznavour', 11814);
INSERT INTO articles_hash VALUES (42,  2, 'ausable', 15885);
INSERT INTO articles_hash VALUES (43,  3, 'affixal', 12723);
INSERT INTO articles_hash VALUES (44,  4, 'anteport', 16793);
INSERT INTO articles_hash VALUES (45,  5, 'afrasia', 864);
INSERT INTO articles_hash VALUES (46,  6, 'atlanta', 17702);
INSERT INTO articles_hash VALUES (47,  7, 'abeyance', 1772);
INSERT INTO articles_hash VALUES (48,  8, 'alkylic', 18610);
INSERT INTO articles_hash VALUES (49,  9, 'anyone', 2681);
INSERT INTO articles_hash VALUES (50, 10, 'anjanette', 19519);



SET citus.task_executor_type TO 'real-time';
SET citus.large_table_shard_count TO 2;
SET client_min_messages TO 'DEBUG2';

-- insert a single row for the test
INSERT INTO articles_single_shard_hash VALUES (50, 10, 'anjanette', 19519);

-- first, test zero-shard SELECT, which should return an empty row
SELECT COUNT(*) FROM articles_hash WHERE author_id = 1 AND author_id = 2;

-- single-shard tests

-- test simple select for a single row
SELECT * FROM articles_hash WHERE author_id = 10 AND id = 50;

-- get all titles by a single author
SELECT title FROM articles_hash WHERE author_id = 10;

-- try ordering them by word count
SELECT title, word_count FROM articles_hash
	WHERE author_id = 10
	ORDER BY word_count DESC NULLS LAST;

-- look at last two articles by an author
SELECT title, id FROM articles_hash
	WHERE author_id = 5
	ORDER BY id
	LIMIT 2;

-- find all articles by two authors in same shard
-- but plan is not router executable due to order by
SELECT title, author_id FROM articles_hash
	WHERE author_id = 7 OR author_id = 8
	ORDER BY author_id ASC, id;

-- same query is router executable with no order by
SELECT title, author_id FROM articles_hash
	WHERE author_id = 7 OR author_id = 8;

-- add in some grouping expressions, still on same shard
-- having queries unsupported in Citus
SELECT author_id, sum(word_count) AS corpus_size FROM articles_hash
	WHERE author_id = 1 OR author_id = 7 OR author_id = 8 OR author_id = 10
	GROUP BY author_id
	HAVING sum(word_count) > 1000
	ORDER BY sum(word_count) DESC;

-- however having clause is supported if it goes to a single shard
SELECT author_id, sum(word_count) AS corpus_size FROM articles_hash
	WHERE author_id = 1
	GROUP BY author_id
	HAVING sum(word_count) > 1000
	ORDER BY sum(word_count) DESC;


-- UNION/INTERSECT queries are unsupported
-- this is rejected by router planner and handled by multi_logical_planner
SELECT * FROM articles_hash WHERE author_id = 10 UNION
SELECT * FROM articles_hash WHERE author_id = 1; 

-- query is a single shard query but can't do shard pruning,
-- not router-plannable due to <= and IN
SELECT * FROM articles_hash WHERE author_id <= 1; 
SELECT * FROM articles_hash WHERE author_id IN (1, 3); 

-- queries using CTEs are unsupported
WITH first_author AS ( SELECT id FROM articles_hash WHERE author_id = 1)
SELECT title FROM articles_hash WHERE author_id = 1;

-- queries which involve functions in FROM clause are unsupported.
SELECT * FROM articles_hash, position('om' in 'Thomas') WHERE author_id = 1;

-- subqueries are not supported in WHERE clause in Citus
SELECT * FROM articles_hash WHERE author_id IN (SELECT id FROM authors_hash WHERE name LIKE '%a');

-- subqueries are supported in FROM clause but they are not router plannable
SELECT articles_hash.id,test.word_count
FROM articles_hash, (SELECT id, word_count FROM articles_hash) AS test WHERE test.id = articles_hash.id
ORDER BY articles_hash.id;

-- subqueries are not supported in SELECT clause
SELECT a.title AS name, (SELECT a2.id FROM authors_hash a2 WHERE a.id = a2.id  LIMIT 1)
						 AS special_price FROM articles_hash a;

-- simple lookup query
SELECT *
	FROM articles_hash
	WHERE author_id = 1;

-- below query hits a single shard, but it is not router plannable
-- still router executable
SELECT *
	FROM articles_hash
	WHERE author_id = 1 OR author_id = 17;

-- below query hits two shards, not router plannable + not router executable
-- handled by real-time executor
SELECT *
	FROM articles_hash
	WHERE author_id = 1 OR author_id = 18;

-- rename the output columns
SELECT id as article_id, word_count * id as random_value
	FROM articles_hash
	WHERE author_id = 1;

-- we can push down co-located joins to a single worker
-- this is not router plannable but router executable
-- handled by real-time executor
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- following join is neither router plannable, nor router executable
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- single shard select with limit is router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	LIMIT 3;

-- single shard select with limit + offset is router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	LIMIT 2
	OFFSET 1;

-- single shard select with limit + offset + order by is router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id desc
	LIMIT 2
	OFFSET 1;
	
-- single shard select with group by on non-partition column is router plannable
SELECT id
	FROM articles_hash
	WHERE author_id = 1
	GROUP BY id;

-- single shard select with distinct is router plannable
SELECT distinct id
	FROM articles_hash
	WHERE author_id = 1;

-- single shard aggregate is router plannable
SELECT avg(word_count)
	FROM articles_hash
	WHERE author_id = 2;

-- max, min, sum, count are router plannable on single shard
SELECT max(word_count) as max, min(word_count) as min,
	   sum(word_count) as sum, count(word_count) as cnt
	FROM articles_hash
	WHERE author_id = 2;


-- queries with aggregates and group by supported on single shard
SELECT max(word_count)
	FROM articles_hash
	WHERE author_id = 1
	GROUP BY author_id;

SET client_min_messages to 'NOTICE';
-- error out for queries with repartition jobs
SELECT *
	FROM articles_hash a, articles_hash b
	WHERE a.id = b.id  AND a.author_id = 1;

-- queries which hit more than 1 shards are not router plannable or executable
-- handled by real-time executor
SELECT *
	FROM articles_hash
	WHERE author_id >= 1 AND author_id <= 3;

SET citus.task_executor_type TO 'real-time';

-- Test various filtering options for router plannable check
SET client_min_messages to 'DEBUG2';

-- this is definitely single shard
-- but not router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and author_id >= 1;

-- not router plannable due to or
SELECT *
	FROM articles_hash
	WHERE author_id = 1 or id = 1;
	
-- router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and (id = 1 or id = 41);

-- router plannable
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and (id = random()::int  * 0);

-- not router plannable due to function call on the right side
SELECT *
	FROM articles_hash
	WHERE author_id = (random()::int  * 0 + 1);

-- not router plannable due to or
SELECT *
	FROM articles_hash
	WHERE author_id = 1 or id = 1;
	
-- router plannable due to abs(-1) getting converted to 1 by postgresql
SELECT *
	FROM articles_hash
	WHERE author_id = abs(-1);

-- not router plannable due to abs() function
SELECT *
	FROM articles_hash
	WHERE 1 = abs(author_id);

-- not router plannable due to abs() function
SELECT *
	FROM articles_hash
	WHERE author_id = abs(author_id - 2);

-- router plannable, function on different field
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and (id = abs(id - 2));

-- not router plannable due to is true
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) is true;

-- router plannable, (boolean expression) = true is collapsed to (boolean expression)
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) = true;

-- router plannable, between operator is on another column
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) and id between 0 and 20;

-- router plannable, partition column expression is and'ed to rest
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) and (id = 1 or id = 31) and title like '%s';

-- router plannable, order is changed
SELECT *
	FROM articles_hash
	WHERE (id = 1 or id = 31) and title like '%s' and (author_id = 1);

-- router plannable
SELECT *
	FROM articles_hash
	WHERE (title like '%s' or title like 'a%') and (author_id = 1);

-- router plannable
SELECT *
	FROM articles_hash
	WHERE (title like '%s' or title like 'a%') and (author_id = 1) and (word_count < 3000 or word_count > 8000);

-- window functions are supported if query is router plannable
SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash
	WHERE author_id = 5;

SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash
	WHERE author_id = 5
	ORDER BY word_count DESC;

SELECT id, MIN(id) over (order by word_count)
	FROM articles_hash
	WHERE author_id = 1;

SELECT id, word_count, AVG(word_count) over (order by word_count)
	FROM articles_hash
	WHERE author_id = 1;

SELECT word_count, rank() OVER (PARTITION BY author_id ORDER BY word_count)  
	FROM articles_hash 
	WHERE author_id = 1;

-- window functions are not supported for not router plannable queries
SELECT id, MIN(id) over (order by word_count)
	FROM articles_hash
	WHERE author_id = 1 or author_id = 2;

	
-- but they are not supported for not router plannable queries
SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash
	WHERE author_id = 5 or author_id = 1;

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
		articles_hash
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
		articles_hash
 	GROUP BY
		author_id;

-- queries inside transactions can be router plannable
BEGIN;
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;
END;

-- cursor queries are router plannable
BEGIN;
DECLARE test_cursor CURSOR FOR 
	SELECT *
		FROM articles_hash
		WHERE author_id = 1
		ORDER BY id;
FETCH test_cursor;
FETCH test_cursor;
END;

-- queries inside copy can be router plannable
COPY (
	SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id) TO STDOUT;
	
-- table creation queries inside can be router plannable
CREATE TEMP TABLE temp_articles_hash as
	SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;

-- router plannable queries may include filter for aggragates
SELECT count(*), count(*) FILTER (WHERE id < 3)
	FROM articles_hash
	WHERE author_id = 1;

-- non-router plannable queries do not support filters
SELECT count(*), count(*) FILTER (WHERE id < 3)
	FROM articles_hash
	WHERE author_id = 1 or author_id = 2;

-- prepare queries can be router plannable
PREPARE author_1_articles as
	SELECT *
	FROM articles_hash
	WHERE author_id = 1;

EXECUTE author_1_articles;

-- parametric prepare queries can be router plannable
PREPARE author_articles(int) as
	SELECT *
	FROM articles_hash
	WHERE author_id = $1;

EXECUTE author_articles(1);

-- queries inside plpgsql functions could be router plannable
CREATE OR REPLACE FUNCTION author_articles_max_id() RETURNS int AS $$
DECLARE
  max_id integer;
BEGIN
	SELECT MAX(id) FROM articles_hash ah
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
		FROM articles_hash ah
		WHERE author_id = 1;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM author_articles_id_word_count();

-- router planner/executor is disabled for task-tracker executor
-- following query is router plannable, but router planner is disabled
SET citus.task_executor_type to 'task-tracker';
SELECT id
	FROM articles_hash
	WHERE author_id = 1;

-- insert query is router plannable even under task-tracker
INSERT INTO articles_hash VALUES (51,  1, 'amateus', 1814);

-- verify insert is successfull (not router plannable and executable)
SELECT id
	FROM articles_hash
	WHERE author_id = 1;

SET client_min_messages to 'NOTICE';

DROP FUNCTION author_articles_max_id();
DROP FUNCTION author_articles_id_word_count();

DROP TABLE articles_hash;
DROP TABLE articles_single_shard_hash;
DROP TABLE authors_hash;
