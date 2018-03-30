
SET citus.next_shard_id TO 840000;


-- ===================================================================
-- test router planner functionality for single shard select queries
-- ===================================================================

CREATE TABLE articles_hash (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer
);

CREATE TABLE articles_range (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer
);

CREATE TABLE articles_append (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer
);

-- Check for the existence of line 'DEBUG:  Creating router plan'
-- to determine if router planner is used.

-- this table is used in a CTE test
CREATE TABLE authors_hash ( name varchar(20), id bigint );
CREATE TABLE authors_range ( name varchar(20), id bigint );
CREATE TABLE authors_reference ( name varchar(20), id bigint );

-- this table is used in router executor tests
CREATE TABLE articles_single_shard_hash (LIKE articles_hash);

SELECT master_create_distributed_table('articles_hash', 'author_id', 'hash');
SELECT master_create_distributed_table('articles_single_shard_hash', 'author_id', 'hash');

-- test when a table is distributed but no shards created yet
SELECT count(*) from articles_hash;

SELECT master_create_worker_shards('articles_hash', 2, 1);
SELECT master_create_worker_shards('articles_single_shard_hash', 1, 1);

SELECT create_reference_table('authors_reference');

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

-- query is a single shard query but can't do shard pruning,
-- not router-plannable due to <= and IN
SELECT * FROM articles_hash WHERE author_id <= 1; 
SELECT * FROM articles_hash WHERE author_id IN (1, 3); 

-- queries with CTEs are supported
WITH first_author AS ( SELECT id FROM articles_hash WHERE author_id = 1)
SELECT * FROM first_author;

-- queries with CTEs are supported even if CTE is not referenced inside query
WITH first_author AS ( SELECT id FROM articles_hash WHERE author_id = 1)
SELECT title FROM articles_hash WHERE author_id = 1;

-- two CTE joins are supported if they go to the same worker
WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 1)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 3)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- CTE joins are supported because they are both planned recursively
WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 2)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- recursive CTEs are supported when filtered on partition column
CREATE TABLE company_employees (company_id int, employee_id int, manager_id int); 
SELECT master_create_distributed_table('company_employees', 'company_id', 'hash');
SELECT master_create_worker_shards('company_employees', 4, 1);

INSERT INTO company_employees values(1, 1, 0);
INSERT INTO company_employees values(1, 2, 1);
INSERT INTO company_employees values(1, 3, 1);
INSERT INTO company_employees values(1, 4, 2);
INSERT INTO company_employees values(1, 5, 4);

INSERT INTO company_employees values(3, 1, 0);
INSERT INTO company_employees values(3, 15, 1);
INSERT INTO company_employees values(3, 3, 1);

-- find employees at top 2 level within company hierarchy
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 1))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- query becomes not router plannble and gets rejected
-- if filter on company is dropped
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- logically wrong query, query involves different shards
-- from the same table
WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 3 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 2))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

-- Test router modifying CTEs
WITH new_article AS (
    INSERT INTO articles_hash VALUES (1,  1, 'arsenous', 9) RETURNING *
)
SELECT * FROM new_article;

WITH update_article AS (
    UPDATE articles_hash SET word_count = 10 WHERE id = 1 AND word_count = 9 RETURNING *
)
SELECT * FROM update_article;

WITH delete_article AS (
    DELETE FROM articles_hash WHERE id = 1 AND word_count = 10 RETURNING *
)
SELECT * FROM delete_article;

-- Modifying statement in nested CTE case is covered by PostgreSQL itself
WITH new_article AS (
    WITH nested_cte AS (
        INSERT INTO articles_hash VALUES (1,  1, 'arsenous', 9572) RETURNING *
    )
    SELECT * FROM nested_cte
)
SELECT * FROM new_article;

-- Modifying statement in a CTE in subquery is also covered by PostgreSQL
SELECT * FROM (
    WITH new_article AS (
        INSERT INTO articles_hash VALUES (1,  1, 'arsenous', 9572) RETURNING *
    )
    SELECT * FROM new_article
) AS subquery_cte;

-- grouping sets are supported on single shard
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash
	WHERE author_id = 1 or author_id = 3
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- grouping sets are not supported on multiple shards
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash
	WHERE author_id = 1 or author_id = 2
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- queries which involve functions in FROM clause are supported if it goes to a single worker.
SELECT * FROM articles_hash, position('om' in 'Thomas') WHERE author_id = 1;

SELECT * FROM articles_hash, position('om' in 'Thomas') WHERE author_id = 1 or author_id = 3;

-- they are supported via (sub)query pushdown if multiple workers are involved
SELECT * FROM articles_hash, position('om' in 'Thomas') WHERE author_id = 1 or author_id = 2 ORDER BY 4 DESC, 1 DESC, 2 DESC LIMIT 5;

-- unless the query can be transformed into a join
SELECT * FROM articles_hash
WHERE author_id IN (SELECT author_id FROM articles_hash WHERE author_id = 2)
ORDER BY articles_hash.id;

-- subqueries are supported in FROM clause but they are not router plannable
SELECT articles_hash.id,test.word_count
FROM articles_hash, (SELECT id, word_count FROM articles_hash) AS test WHERE test.id = articles_hash.id
ORDER BY test.word_count DESC, articles_hash.id LIMIT 5;


SELECT articles_hash.id,test.word_count
FROM articles_hash, (SELECT id, word_count FROM articles_hash) AS test 
WHERE test.id = articles_hash.id and articles_hash.author_id = 1
ORDER BY articles_hash.id;

-- subqueries are not supported in SELECT clause
SELECT a.title AS name, (SELECT a2.id FROM articles_single_shard_hash a2 WHERE a.id = a2.id  LIMIT 1)
						 AS special_price FROM articles_hash a;

-- simple lookup query
SELECT *
	FROM articles_hash
	WHERE author_id = 1;

-- below query hits a single shard, router plannable
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
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- following join is router plannable since the same worker 
-- has both shards
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;
	
-- following join is not router plannable since there are no
-- workers containing both shards, but will work through recursive
-- planning
WITH single_shard as (SELECT * FROM articles_single_shard_hash)
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, single_shard b
	WHERE a.author_id = 2 and a.author_id = b.author_id
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
	GROUP BY id
	ORDER BY id;

-- single shard select with distinct is router plannable
SELECT DISTINCT id
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;

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


-- router plannable union queries are supported
SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash WHERE author_id = 3
) AS combination
ORDER BY id;

(SELECT LEFT(title, 1) FROM articles_hash WHERE author_id = 1)
UNION
(SELECT LEFT(title, 1) FROM articles_hash WHERE author_id = 3);

(SELECT LEFT(title, 1) FROM articles_hash WHERE author_id = 1)
INTERSECT
(SELECT LEFT(title, 1) FROM articles_hash WHERE author_id = 3);

SELECT * FROM (
	SELECT LEFT(title, 2) FROM articles_hash WHERE author_id = 1
	EXCEPT
	SELECT LEFT(title, 2) FROM articles_hash WHERE author_id = 3
) AS combination
ORDER BY 1;

-- top-level union queries are supported through recursive planning

SET client_min_messages to 'NOTICE';

(
  (SELECT * FROM articles_hash WHERE author_id = 1)
  UNION
  (SELECT * FROM articles_hash WHERE author_id = 3)
)
UNION
(SELECT * FROM articles_hash WHERE author_id = 2)
ORDER BY 1,2,3;

-- unions in subqueries are supported with subquery pushdown
SELECT * FROM (
	(SELECT * FROM articles_hash WHERE author_id = 1)
	UNION
	(SELECT * FROM articles_hash WHERE author_id = 2)) uu
ORDER BY 1, 2
LIMIT 5;

-- error out for queries with repartition jobs
SELECT *
	FROM articles_hash a, articles_hash b
	WHERE a.id = b.id  AND a.author_id = 1;

-- by setting enable_repartition_joins we can make this query run
SET citus.enable_repartition_joins TO ON;
SELECT *
	FROM articles_hash a, articles_hash b
	WHERE a.id = b.id  AND a.author_id = 1;
SET citus.enable_repartition_joins TO OFF;
-- queries which hit more than 1 shards are not router plannable or executable
-- handled by real-time executor
SELECT *
	FROM articles_hash
	WHERE author_id >= 1 AND author_id <= 3;

SET citus.task_executor_type TO 'real-time';

-- Test various filtering options for router plannable check
SET client_min_messages to 'DEBUG2';

-- this is definitely single shard
-- and router plannable
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

SELECT LAG(title, 1) over (ORDER BY word_count) prev, title, word_count 
	FROM articles_hash
	WHERE author_id = 5 or author_id = 2;

-- where false queries are router plannable
SELECT * 
	FROM articles_hash
	WHERE false;

SELECT * 
	FROM articles_hash
	WHERE author_id = 1 and false;

SELECT * 
	FROM articles_hash
	WHERE author_id = 1 and 1=0;

SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id and false;

SELECT * 
	FROM articles_hash
	WHERE null;

-- where false with immutable function returning false
SELECT * 
	FROM articles_hash a
	WHERE a.author_id = 10 and int4eq(1, 2);

SELECT * 
	FROM articles_hash a
	WHERE int4eq(1, 2);

SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id and int4eq(1, 1);

SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id and int4eq(1, 2);

-- partition_column is null clause does not prune out any shards,
-- all shards remain after shard pruning, not router plannable
SELECT * 
	FROM articles_hash a
	WHERE a.author_id is null;

-- partition_column equals to null clause prunes out all shards
-- no shards after shard pruning, router plannable
SELECT * 
	FROM articles_hash a
	WHERE a.author_id = null;

-- stable function returning bool
SELECT * 
	FROM articles_hash a
	WHERE date_ne_timestamp('1954-04-11', '1954-04-11'::timestamp);

SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_single_shard_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id and
		date_ne_timestamp('1954-04-11', '1954-04-11'::timestamp);

-- union/difference /intersection with where false
-- this query was not originally router plannable, addition of 1=0
-- makes it router plannable
SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash WHERE author_id = 2 and 1=0
) AS combination
ORDER BY id;

SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	EXCEPT
	SELECT * FROM articles_hash WHERE author_id = 2 and 1=0
) AS combination
ORDER BY id;


(SELECT * FROM articles_hash WHERE author_id = 1)
INTERSECT
(SELECT * FROM articles_hash WHERE author_id = 2 and 1=0);

-- CTEs with where false
WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 1 and 1=0)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 1)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id and 1=0;

WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 1))
SELECT * FROM hierarchy WHERE LEVEL <= 2 and 1=0;

WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 1 and manager_id = 0 
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 1 AND 1=0))
SELECT * FROM hierarchy WHERE LEVEL <= 2;

WITH RECURSIVE hierarchy as (
	SELECT *, 1 AS level
		FROM company_employees
		WHERE company_id = 1 and manager_id = 0 AND 1=0
	UNION
	SELECT ce.*, (h.level+1)
		FROM hierarchy h JOIN company_employees ce
			ON (h.employee_id = ce.manager_id AND
				h.company_id = ce.company_id AND
				ce.company_id = 1))
SELECT * FROM hierarchy WHERE LEVEL <= 2;


-- window functions with where false
SELECT word_count, rank() OVER (PARTITION BY author_id ORDER BY word_count)  
	FROM articles_hash 
	WHERE author_id = 1 and 1=0;

-- function calls in WHERE clause with non-relational arguments
SELECT author_id FROM articles_hash
	WHERE 
		substring('hello world', 1, 5) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

-- when expression evaluates to false
SELECT author_id FROM articles_hash
	WHERE 
		substring('hello world', 1, 4) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

	
-- verify range partitioned tables can be used in router plannable queries
-- just 4 shards to be created for each table to make sure 
-- they are 'co-located' pairwise
SET citus.shard_replication_factor TO 1;
SELECT master_create_distributed_table('authors_range', 'id', 'range');
SELECT master_create_distributed_table('articles_range', 'author_id', 'range');

SELECT master_create_empty_shard('authors_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue=10 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('authors_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 11, shardmaxvalue=30 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('authors_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 21, shardmaxvalue=40 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('authors_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 31, shardmaxvalue=40 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('articles_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue=10 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('articles_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 11, shardmaxvalue=30 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('articles_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 21, shardmaxvalue=40 WHERE shardid = :shard_id;

SELECT master_create_empty_shard('articles_range') as shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = 31, shardmaxvalue=40 WHERE shardid = :shard_id;

-- single shard select queries are router plannable
SELECT * FROM articles_range where author_id = 1;
SELECT * FROM articles_range where author_id = 1 or author_id = 5;

-- zero shard select query is router plannable
SELECT * FROM articles_range where author_id = 1 and author_id = 2;

-- single shard joins on range partitioned table are router plannable
SELECT * FROM articles_range ar join authors_range au on (ar.author_id = au.id) 
	WHERE ar.author_id = 1;

-- zero shard join is router plannable
SELECT * FROM articles_range ar join authors_range au on (ar.author_id = au.id)
	WHERE ar.author_id = 1 and au.id = 2;

-- This query was intended to test "multi-shard join is not router plannable"
-- To run it using repartition join logic we change the join columns
SET citus.task_executor_type to "task-tracker";
SELECT * FROM articles_range ar join authors_range au on (ar.title = au.name)
	WHERE ar.author_id = 35;

-- This query was intended to test "this is a bug, it is a single shard join
-- query but not router plannable". To run it using repartition join logic we
-- change the join columns.
SELECT * FROM articles_range ar join authors_range au on (ar.title = au.name) 
	WHERE ar.author_id = 1 or au.id = 5;
RESET citus.task_executor_type;

-- bogus query, join on non-partition column, but router plannable due to filters
SELECT * FROM articles_range ar join authors_range au on (ar.id = au.id) 
	WHERE ar.author_id = 1 and au.id < 10;

-- join between hash and range partition tables are router plannable
-- only if both tables pruned down to single shard and co-located on the same
-- node.
-- router plannable
SELECT * FROM articles_hash ar join authors_range au on (ar.author_id = au.id)
	WHERE ar.author_id = 2;

-- not router plannable
SELECT * FROM articles_hash ar join authors_range au on (ar.author_id = au.id)
	WHERE ar.author_id = 3;

-- join between a range partitioned table and reference table is router plannable
SELECT * FROM articles_range ar join authors_reference au on (ar.author_id = au.id)
	WHERE ar.author_id = 1;

-- still hits a single shard and router plannable
SELECT * FROM articles_range ar join authors_reference au on (ar.author_id = au.id)
	WHERE ar.author_id = 1 or ar.author_id = 5;

-- it is not router plannable if hit multiple shards
SELECT * FROM articles_range ar join authors_reference au on (ar.author_id = au.id)
	WHERE ar.author_id = 1 or ar.author_id = 15;
	
-- following is a bug, function should have been
-- evaluated at master before going to worker
-- need to use a append distributed table here
SELECT master_create_distributed_table('articles_append', 'author_id', 'append');
SET citus.shard_replication_factor TO 1;
SELECT master_create_empty_shard('articles_append') AS shard_id \gset
UPDATE pg_dist_shard SET shardmaxvalue = 100, shardminvalue=1 WHERE shardid = :shard_id;

SELECT author_id FROM articles_append
	WHERE 
		substring('articles_append'::regclass::text, 1, 5) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

-- same query with where false but evaluation left to worker
SELECT author_id FROM articles_append
	WHERE 
		substring('articles_append'::regclass::text, 1, 4) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

-- same query on router planner with where false but evaluation left to worker
SELECT author_id FROM articles_single_shard_hash
	WHERE 
		substring('articles_single_shard_hash'::regclass::text, 1, 4) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

SELECT author_id FROM articles_hash
	WHERE 
		author_id = 1
		AND substring('articles_hash'::regclass::text, 1, 5) = 'hello'
	ORDER BY
		author_id
	LIMIT 1;

-- create a dummy function to be used in filtering
CREATE OR REPLACE FUNCTION someDummyFunction(regclass)
    RETURNS text AS
$$
BEGIN
    RETURN md5($1::text);
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- not router plannable, returns all rows 
SELECT * FROM articles_hash
	WHERE
		someDummyFunction('articles_hash') = md5('articles_hash')
	ORDER BY
		author_id, id
	LIMIT 5;

-- router plannable, errors
SELECT * FROM articles_hash
	WHERE
		someDummyFunction('articles_hash') = md5('articles_hash') AND author_id = 1
	ORDER BY
		author_id, id
	LIMIT 5;

-- temporarily turn off debug messages before dropping the function
SET client_min_messages TO 'NOTICE';
DROP FUNCTION someDummyFunction(regclass);

SET client_min_messages TO 'DEBUG2';

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
		author_id
	ORDER BY c;

-- queries inside transactions can be router plannable
BEGIN;
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;
END;

-- queries inside read-only transactions can be router plannable
BEGIN;
SET TRANSACTION READ ONLY;
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
FETCH ALL test_cursor;
FETCH test_cursor; -- fetch one row after the last
FETCH BACKWARD test_cursor;
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

-- non-router plannable queries also support filters
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

-- check that function returning setof query are router plannable
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

-- materialized views can be created for router plannable queries
CREATE MATERIALIZED VIEW mv_articles_hash_empty AS
	SELECT * FROM articles_hash WHERE author_id = 1;
SELECT * FROM mv_articles_hash_empty;

CREATE MATERIALIZED VIEW mv_articles_hash_data AS
	SELECT * FROM articles_hash WHERE author_id in (1,2);
SELECT * FROM mv_articles_hash_data;

-- router planner/executor is now enabled for task-tracker executor
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

-- test that a connection failure marks placements invalid
SET citus.shard_replication_factor TO 2;
CREATE TABLE failure_test (a int, b int);
SELECT master_create_distributed_table('failure_test', 'a', 'hash');
SELECT master_create_worker_shards('failure_test', 2);

CREATE USER router_user;
GRANT INSERT ON ALL TABLES IN SCHEMA public TO router_user;
\c - - - :worker_1_port
CREATE USER router_user;
GRANT INSERT ON ALL TABLES IN SCHEMA public TO router_user;
\c - router_user - :master_port
-- first test that it is marked invalid inside a transaction block
-- we will fail to connect to worker 2, since the user does not exist
BEGIN;
INSERT INTO failure_test VALUES (1, 1);
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement
	WHERE shardid IN (
		SELECT shardid FROM pg_dist_shard
		WHERE logicalrelid = 'failure_test'::regclass
	)
	ORDER BY placementid;
ROLLBACK;
INSERT INTO failure_test VALUES (2, 1);
SELECT shardid, shardstate, nodename, nodeport FROM pg_dist_shard_placement
	WHERE shardid IN (
		SELECT shardid FROM pg_dist_shard
		WHERE logicalrelid = 'failure_test'::regclass
	)
	ORDER BY placementid;
\c - postgres - :worker_1_port
DROP OWNED BY router_user;
DROP USER router_user;
\c - - - :master_port
DROP OWNED BY router_user;
DROP USER router_user;
DROP TABLE failure_test;

DROP FUNCTION author_articles_max_id();
DROP FUNCTION author_articles_id_word_count();

DROP MATERIALIZED VIEW mv_articles_hash_empty;
DROP MATERIALIZED VIEW mv_articles_hash_data;

DROP TABLE articles_hash;
DROP TABLE articles_single_shard_hash;
DROP TABLE authors_hash;
DROP TABLE authors_range;
DROP TABLE authors_reference;
DROP TABLE company_employees;
DROP TABLE articles_range;
DROP TABLE articles_append;
