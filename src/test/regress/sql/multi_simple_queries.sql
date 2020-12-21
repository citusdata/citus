
SET citus.next_shard_id TO 850000;

-- many of the tests in this file is intended for testing non-fast-path
-- router planner, so we're explicitly disabling it in this file.
-- We've bunch of other tests that triggers fast-path-router
SET citus.enable_fast_path_router_planner TO false;
SET citus.coordinator_aggregation_strategy TO 'disabled';

-- ===================================================================
-- test end-to-end query functionality
-- ===================================================================

CREATE TABLE articles (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer NOT NULL CHECK (word_count > 0)
);

-- this table is used in a CTE test
CREATE TABLE authors ( name text, id bigint );

-- this table is used in router executor tests
CREATE TABLE articles_single_shard (LIKE articles);

SELECT master_create_distributed_table('articles', 'author_id', 'hash');
SELECT master_create_distributed_table('articles_single_shard', 'author_id', 'hash');

SELECT master_create_worker_shards('articles', 2, 1);
SELECT master_create_worker_shards('articles_single_shard', 1, 1);

-- create a bunch of test data
INSERT INTO articles VALUES ( 1,  1, 'arsenous', 9572);
INSERT INTO articles VALUES ( 2,  2, 'abducing', 13642);
INSERT INTO articles VALUES ( 3,  3, 'asternal', 10480);
INSERT INTO articles VALUES ( 4,  4, 'altdorfer', 14551);
INSERT INTO articles VALUES ( 5,  5, 'aruru', 11389);
INSERT INTO articles VALUES ( 6,  6, 'atlases', 15459);
INSERT INTO articles VALUES ( 7,  7, 'aseptic', 12298);
INSERT INTO articles VALUES ( 8,  8, 'agatized', 16368);
INSERT INTO articles VALUES ( 9,  9, 'alligate', 438);
INSERT INTO articles VALUES (10, 10, 'aggrandize', 17277);
INSERT INTO articles VALUES (11,  1, 'alamo', 1347);
INSERT INTO articles VALUES (12,  2, 'archiblast', 18185);
INSERT INTO articles VALUES (13,  3, 'aseyev', 2255);
INSERT INTO articles VALUES (14,  4, 'andesite', 19094);
INSERT INTO articles VALUES (15,  5, 'adversa', 3164);
INSERT INTO articles VALUES (16,  6, 'allonym', 2);
INSERT INTO articles VALUES (17,  7, 'auriga', 4073);
INSERT INTO articles VALUES (18,  8, 'assembly', 911);
INSERT INTO articles VALUES (19,  9, 'aubergiste', 4981);
INSERT INTO articles VALUES (20, 10, 'absentness', 1820);
INSERT INTO articles VALUES (21,  1, 'arcading', 5890);
INSERT INTO articles VALUES (22,  2, 'antipope', 2728);
INSERT INTO articles VALUES (23,  3, 'abhorring', 6799);
INSERT INTO articles VALUES (24,  4, 'audacious', 3637);
INSERT INTO articles VALUES (25,  5, 'antehall', 7707);
INSERT INTO articles VALUES (26,  6, 'abington', 4545);
INSERT INTO articles VALUES (27,  7, 'arsenous', 8616);
INSERT INTO articles VALUES (28,  8, 'aerophyte', 5454);
INSERT INTO articles VALUES (29,  9, 'amateur', 9524);
INSERT INTO articles VALUES (30, 10, 'andelee', 6363);
INSERT INTO articles VALUES (31,  1, 'athwartships', 7271);
INSERT INTO articles VALUES (32,  2, 'amazon', 11342);
INSERT INTO articles VALUES (33,  3, 'autochrome', 8180);
INSERT INTO articles VALUES (34,  4, 'amnestied', 12250);
INSERT INTO articles VALUES (35,  5, 'aminate', 9089);
INSERT INTO articles VALUES (36,  6, 'ablation', 13159);
INSERT INTO articles VALUES (37,  7, 'archduchies', 9997);
INSERT INTO articles VALUES (38,  8, 'anatine', 14067);
INSERT INTO articles VALUES (39,  9, 'anchises', 10906);
INSERT INTO articles VALUES (40, 10, 'attemper', 14976);
INSERT INTO articles VALUES (41,  1, 'aznavour', 11814);
INSERT INTO articles VALUES (42,  2, 'ausable', 15885);
INSERT INTO articles VALUES (43,  3, 'affixal', 12723);
INSERT INTO articles VALUES (44,  4, 'anteport', 16793);
INSERT INTO articles VALUES (45,  5, 'afrasia', 864);
INSERT INTO articles VALUES (46,  6, 'atlanta', 17702);
INSERT INTO articles VALUES (47,  7, 'abeyance', 1772);
INSERT INTO articles VALUES (48,  8, 'alkylic', 18610);
INSERT INTO articles VALUES (49,  9, 'anyone', 2681);
INSERT INTO articles VALUES (50, 10, 'anjanette', 19519);

-- insert a single row for the test
INSERT INTO articles_single_shard VALUES (50, 10, 'anjanette', 19519);

-- zero-shard modifications should succeed
UPDATE articles SET title = '' WHERE author_id = 1 AND author_id = 2;
UPDATE articles SET title = '' WHERE 0 = 1;
DELETE FROM articles WHERE author_id = 1 AND author_id = 2;

-- single-shard tests

-- test simple select for a single row
SELECT * FROM articles WHERE author_id = 10 AND id = 50;

-- get all titles by a single author
SELECT title FROM articles WHERE author_id = 10;

-- try ordering them by word count
SELECT title, word_count FROM articles
	WHERE author_id = 10
	ORDER BY word_count DESC NULLS LAST;

-- look at last two articles by an author
SELECT title, id FROM articles
	WHERE author_id = 5
	ORDER BY id
	LIMIT 2;

-- find all articles by two authors in same shard
SELECT title, author_id FROM articles
	WHERE author_id = 7 OR author_id = 8
	ORDER BY author_id ASC, id;

-- add in some grouping expressions
SELECT author_id, sum(word_count) AS corpus_size FROM articles
	WHERE author_id = 1 OR author_id = 2 OR author_id = 8 OR author_id = 10
	GROUP BY author_id
	HAVING sum(word_count) > 40000
	ORDER BY sum(word_count) DESC;

-- UNION/INTERSECT queries are supported if on multiple shards
SELECT * FROM articles WHERE author_id = 10 UNION
SELECT * FROM articles WHERE author_id = 2
ORDER BY 1,2,3;

-- queries using CTEs are supported
WITH long_names AS ( SELECT id FROM authors WHERE char_length(name) > 15 )
SELECT title FROM articles ORDER BY 1 LIMIT 5;

-- queries which involve functions in FROM clause are recursively planned
SELECT * FROM articles, position('om' in 'Thomas') ORDER BY 2 DESC, 1 DESC, 3 DESC LIMIT 5;

-- subqueries are supported in WHERE clause in Citus even if the relations are not distributed
SELECT * FROM articles WHERE author_id IN (SELECT id FROM authors WHERE name LIKE '%a');

-- subqueries are supported in FROM clause

SELECT articles.id,test.word_count
FROM articles, (SELECT id, word_count FROM articles) AS test WHERE test.id = articles.id
ORDER BY articles.id;

-- subqueries are not supported in SELECT clause
SELECT a.title AS name, (SELECT a2.id FROM articles_single_shard a2 WHERE a.id = a2.id  LIMIT 1)
						 AS special_price FROM articles a;

-- joins are supported between local and distributed tables
SELECT title, authors.name FROM authors, articles WHERE authors.id = articles.author_id;

-- inner joins are supported
SELECT * FROM  (articles INNER JOIN authors ON articles.id = authors.id);

-- test use of EXECUTE statements within plpgsql
DO $sharded_execute$
	BEGIN
		EXECUTE 'SELECT COUNT(*) FROM articles ' ||
				'WHERE author_id = $1 AND author_id = $2' USING 1, 2;
	END
$sharded_execute$;

-- test use of bare SQL within plpgsql
DO $sharded_sql$
	BEGIN
		SELECT COUNT(*) FROM articles WHERE author_id = 1 AND author_id = 2;
	END
$sharded_sql$;

-- test cross-shard queries
SELECT COUNT(*) FROM articles;

-- test with empty target list
SELECT FROM articles;

SELECT FROM articles WHERE author_id = 3737;

SELECT FROM articles WHERE word_count = 65500;

-- having queries supported in Citus
SELECT author_id, sum(word_count) AS corpus_size FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 25000
	ORDER BY sum(word_count) DESC
	LIMIT 5;

SELECT author_id FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 50000
	ORDER BY author_id;

SELECT author_id FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 50000 AND author_id < 5
	ORDER BY author_id;

SELECT author_id FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 50000 OR author_id < 5
	ORDER BY author_id;

SELECT author_id FROM articles
	GROUP BY author_id
	HAVING author_id <= 2 OR author_id = 8
	ORDER BY author_id;

SELECT o_orderstatus, count(*), avg(o_totalprice) FROM orders
	GROUP BY o_orderstatus
	HAVING count(*) > 1450 OR avg(o_totalprice) > 150000
	ORDER BY o_orderstatus;

SELECT o_orderstatus, sum(l_linenumber), avg(l_linenumber) FROM lineitem, orders
	WHERE l_orderkey = o_orderkey AND l_orderkey > 9030
	GROUP BY o_orderstatus
	HAVING sum(l_linenumber) > 1000
	ORDER BY o_orderstatus;

-- now, test the cases where Citus do or do not need to create
-- the master queries
SET client_min_messages TO 'DEBUG2';

-- start with the simple lookup query
SELECT *
	FROM articles
	WHERE author_id = 1;

-- below query hits a single shard, so no need to create the master query
SELECT *
	FROM articles
	WHERE author_id = 1 OR author_id = 17;

-- below query hits two shards, so needs to create the master query
SELECT *
	FROM articles
	WHERE author_id = 1 OR author_id = 18;

-- rename the output columns on a no master query case
SELECT id as article_id, word_count * id as random_value
	FROM articles
	WHERE author_id = 1;

-- we can push down co-located joins to a single worker without the
-- master query being required for only the same tables
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles a, articles b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- now show that JOINs with multiple tables are not router executable
-- they are executed by real-time executor
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles a, articles_single_shard b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- do not create the master query for LIMIT on a single shard SELECT
SELECT *
	FROM articles
	WHERE author_id = 1
	LIMIT 2;

-- This query hits a single shard. So GROUP BY can be
-- pushed down to the workers directly. This query is
-- equivalent to SELECT DISTINCT on a single shard.
SELECT id
	FROM articles
	WHERE author_id = 1
	GROUP BY id
	ORDER BY id;

-- copying from a single shard table does not require the master query
COPY articles_single_shard TO stdout;

SELECT avg(word_count)
	FROM articles
	WHERE author_id = 2;

-- error out on unsupported aggregate
SET client_min_messages to 'NOTICE';

CREATE AGGREGATE public.invalid(int) (
    sfunc = int4pl,
    stype = int
);

SELECT invalid(word_count) FROM articles;

DROP AGGREGATE invalid(int);

SET client_min_messages to 'DEBUG2';

-- max, min, sum, count is somehow implemented
-- differently in distributed planning
SELECT max(word_count) as max, min(word_count) as min,
	   sum(word_count) as sum, count(word_count) as cnt
	FROM articles
	WHERE author_id = 2;

-- error out for queries with repartition jobs
SELECT *
	FROM articles a, articles b
	WHERE a.id = b.id  AND a.author_id = 1;

-- system columns from shard tables can be queried and retrieved
SELECT count(*) FROM (
    SELECT tableoid, ctid, cmin, cmax, xmin, xmax
        FROM articles
        WHERE tableoid IS NOT NULL OR
                  ctid IS NOT NULL OR
                  cmin IS NOT NULL OR
                  cmax IS NOT NULL OR
                  xmin IS NOT NULL OR
                  xmax IS NOT NULL
) x;

-- tablesample is supported
SELECT * FROM articles TABLESAMPLE SYSTEM (0) WHERE author_id = 1;
SELECT * FROM articles TABLESAMPLE BERNOULLI (0) WHERE author_id = 1;
SELECT * FROM articles TABLESAMPLE SYSTEM (100) WHERE author_id = 1 ORDER BY id;
SELECT * FROM articles TABLESAMPLE BERNOULLI (100) WHERE author_id = 1 ORDER BY id;

-- test tablesample with fast path as well
SET citus.enable_fast_path_router_planner TO true;
SELECT * FROM articles TABLESAMPLE SYSTEM (0) WHERE author_id = 1;
SELECT * FROM articles TABLESAMPLE BERNOULLI (0) WHERE author_id = 1;
SELECT * FROM articles TABLESAMPLE SYSTEM (100) WHERE author_id = 1 ORDER BY id;
SELECT * FROM articles TABLESAMPLE BERNOULLI (100) WHERE author_id = 1 ORDER BY id;

SET client_min_messages to 'NOTICE';

-- we should be able to use nextval in the target list
CREATE SEQUENCE query_seq;
SELECT nextval('query_seq') FROM articles WHERE author_id = 1;
SELECT nextval('query_seq') FROM articles LIMIT 3;
SELECT nextval('query_seq')*2 FROM articles LIMIT 3;
SELECT * FROM (SELECT nextval('query_seq') FROM articles LIMIT 3) vals;

-- but not elsewhere
SELECT sum(nextval('query_seq')) FROM articles;
SELECT n FROM (SELECT nextval('query_seq') n, random() FROM articles) vals;

DROP SEQUENCE query_seq;
