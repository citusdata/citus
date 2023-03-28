SET search_path TO arbitrary_configs_router;

SET client_min_messages TO WARNING;

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
-- but plan is not fast path router plannable due to
-- two distribution columns in the query
SELECT title, author_id FROM articles_hash
	WHERE author_id = 7 OR author_id = 8
	ORDER BY author_id ASC, id;

-- having clause is supported if it goes to a single shard
-- and single dist. key on the query
SELECT author_id, sum(word_count) AS corpus_size FROM articles_hash
	WHERE author_id = 1
	GROUP BY author_id
	HAVING sum(word_count) > 1000
	ORDER BY sum(word_count) DESC;

-- fast path planner only support = operator
SELECT * FROM articles_hash WHERE author_id <= 1;
SELECT * FROM articles_hash WHERE author_id IN (1, 3) ORDER BY 1,2,3,4;

-- queries with CTEs cannot go through fast-path planning
WITH first_author AS ( SELECT id FROM articles_hash WHERE author_id = 1)
SELECT * FROM first_author;

-- two CTE joins also cannot go through fast-path planning
WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 1)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- this is a different case where each CTE is recursively planned and those goes
-- through the fast-path router planner, but the top level join is not
WITH id_author AS ( SELECT id, author_id FROM articles_hash WHERE author_id = 1),
id_title AS (SELECT id, title from articles_hash WHERE author_id = 2)
SELECT * FROM id_author, id_title WHERE id_author.id = id_title.id;

-- recursive CTEs are also cannot go through fast
-- path planning
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

WITH update_article AS (
    UPDATE articles_hash SET word_count = 10 WHERE id = 1 AND word_count = 9 RETURNING *
)
SELECT * FROM update_article;

WITH delete_article AS (
    DELETE FROM articles_hash WHERE id = 1 AND word_count = 10 RETURNING *
)
SELECT * FROM delete_article;

-- grouping sets are supported via fast-path
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash
	WHERE author_id = 1
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- queries which involve functions in FROM clause are not supported via fast path planning
SELECT * FROM articles_hash, position('om' in 'Thomas') WHERE author_id = 1;

-- sublinks are not supported via fast path planning
SELECT * FROM articles_hash
WHERE author_id IN (SELECT author_id FROM articles_hash WHERE author_id = 2)
ORDER BY articles_hash.id;

-- subqueries are not supported via fast path planning
SELECT articles_hash.id,test.word_count
FROM articles_hash, (SELECT id, word_count FROM articles_hash) AS test WHERE test.id = articles_hash.id
ORDER BY test.word_count DESC, articles_hash.id LIMIT 5;

SELECT articles_hash.id,test.word_count
FROM articles_hash, (SELECT id, word_count FROM articles_hash) AS test
WHERE test.id = articles_hash.id and articles_hash.author_id = 1
ORDER BY articles_hash.id;

-- simple lookup query just works
SELECT *
	FROM articles_hash
	WHERE author_id = 1;

-- below query hits a single shard but with multiple filters
-- so cannot go via fast-path
SELECT *
	FROM articles_hash
	WHERE author_id = 1 OR author_id = 17;

-- rename the output columns
SELECT id as article_id, word_count * id as random_value
	FROM articles_hash
	WHERE author_id = 1;

-- joins do not go through fast-path planning
SELECT a.author_id as first_author, b.word_count as second_word_count
	FROM articles_hash a, articles_hash b
	WHERE a.author_id = 10 and a.author_id = b.author_id
	LIMIT 3;

-- single shard select with limit goes through fast-path planning
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	LIMIT 3;

-- single shard select with limit + offset goes through fast-path planning
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	LIMIT 2
	OFFSET 1;

-- single shard select with limit + offset + order by goes through fast-path planning
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id desc
	LIMIT 2
	OFFSET 1;

-- single shard select with group by on non-partition column goes through fast-path planning
SELECT id
	FROM articles_hash
	WHERE author_id = 1
	GROUP BY id
	ORDER BY id;

-- single shard select with distinct goes through fast-path planning
SELECT DISTINCT id
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;

-- single shard aggregate goes through fast-path planning
SELECT avg(word_count)
	FROM articles_hash
	WHERE author_id = 2;

-- max, min, sum, count goes through fast-path planning
SELECT max(word_count) as max, min(word_count) as min,
	   sum(word_count) as sum, count(word_count) as cnt
	FROM articles_hash
	WHERE author_id = 2;


-- queries with aggregates and group by goes through fast-path planning
SELECT max(word_count)
	FROM articles_hash
	WHERE author_id = 1
	GROUP BY author_id;


-- set operations are not supported via fast-path planning
SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash WHERE author_id = 3
) AS combination
ORDER BY id;

-- function calls in the target list is supported via fast path
SELECT LEFT(title, 1) FROM articles_hash WHERE author_id = 1;


-- top-level union queries are supported through recursive planning

-- unions in subqueries are not supported via fast-path planning
SELECT * FROM (
	(SELECT * FROM articles_hash WHERE author_id = 1)
	UNION
	(SELECT * FROM articles_hash WHERE author_id = 1)) uu
ORDER BY 1, 2
LIMIT 5;


-- Test various filtering options for router plannable check

-- cannot go through fast-path if there is
-- explicit coercion
SELECT *
	FROM articles_hash
	WHERE author_id = 1::bigint;

-- can go through fast-path if there is
-- implicit coercion
-- This doesn't work see the related issue
-- reported https://github.com/citusdata/citus/issues/2605
-- SELECT *
--	FROM articles_hash
--	WHERE author_id = 1.0;

SELECT *
	FROM articles_hash
	WHERE author_id = 68719476736; -- this is bigint

-- cannot go through fast-path due to
-- multiple filters on the dist. key
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and author_id >= 1;

-- cannot go through fast-path due to
-- multiple filters on the dist. key
SELECT *
	FROM articles_hash
	WHERE author_id = 1 or id = 1;

-- goes through fast-path planning because
-- the dist. key is ANDed with the rest of the
-- filters
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and (id = 1 or id = 41);

-- this time there is an OR clause which prevents
-- router planning at all
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and id = 1 or id = 41;

-- goes through fast-path planning because
-- the dist. key is ANDed with the rest of the
-- filters
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and (id = random()::int  * 0);

-- not router plannable due to function call on the right side
SELECT *
	FROM articles_hash
	WHERE author_id = (random()::int  * 0 + 1);

-- Citus does not qualify this as a fast-path because
-- dist_key = func()
SELECT *
	FROM articles_hash
	WHERE author_id = abs(-1);

-- Citus does not qualify this as a fast-path because
-- dist_key = func()
SELECT *
	FROM articles_hash
	WHERE 1 = abs(author_id);

-- Citus does not qualify this as a fast-path because
-- dist_key = func()
SELECT *
	FROM articles_hash
	WHERE author_id = abs(author_id - 2);

-- the function is not on the dist. key, so qualify as
-- fast-path
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

-- some more complex quals
SELECT count(*) FROM articles_hash WHERE (author_id = 15) AND (id = 1 OR word_count > 5);
SELECT count(*) FROM articles_hash WHERE (author_id = 15) OR (id = 1 AND word_count > 5);
SELECT count(*) FROM articles_hash WHERE (id = 15) OR (author_id = 1 AND word_count > 5);
SELECT count(*) FROM articles_hash WHERE (id = 15) AND (author_id = 1 OR word_count > 5);
SELECT count(*) FROM articles_hash WHERE (id = 15) AND (author_id = 1 AND (word_count > 5 OR id  = 2));
SELECT count(*) FROM articles_hash WHERE (id = 15) AND (title ilike 'a%' AND (word_count > 5 OR author_id  = 2));
SELECT count(*) FROM articles_hash WHERE (id = 15) AND (title ilike 'a%' AND (word_count > 5 AND author_id  = 2));
SELECT count(*) FROM articles_hash WHERE (id = 15) AND (title ilike 'a%' AND ((word_count > 5 OR title ilike 'b%' ) AND (author_id  = 2 AND word_count > 50)));

-- fast-path router plannable, between operator is on another column
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) and id between 0 and 20;

-- fast-path router plannable, partition column expression is and'ed to rest
SELECT *
	FROM articles_hash
	WHERE (author_id = 1) and (id = 1 or id = 31) and title like '%s';

-- fast-path router plannable, order is changed
SELECT *
	FROM articles_hash
	WHERE (id = 1 or id = 31) and title like '%s' and (author_id = 1);

-- fast-path router plannable
SELECT *
	FROM articles_hash
	WHERE (title like '%s' or title like 'a%') and (author_id = 1);

-- fast-path router plannable
SELECT *
	FROM articles_hash
	WHERE (title like '%s' or title like 'a%') and (author_id = 1) and (word_count < 3000 or word_count > 8000);

-- window functions are supported with fast-path router plannable
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

-- some more tests on complex target lists
SELECT DISTINCT ON (author_id, id) author_id, id,
	MIN(id) over (order by avg(word_count)) * AVG(id * 5.2 + (1.0/max(word_count))) over (order by max(word_count)) as t1,
	count(*) FILTER (WHERE title LIKE 'al%') as cnt_with_filter,
	count(*) FILTER (WHERE '0300030' LIKE '%3%') as cnt_with_filter_2,
	avg(case when id > 2 then char_length(word_count::text) * (id * strpos(word_count::text, '1')) end) as case_cnt,
	COALESCE(strpos(avg(word_count)::text, '1'), 20)
	FROM articles_hash as aliased_table
	WHERE author_id = 1
	GROUP BY author_id, id
	HAVING count(DISTINCT title) > 0
	ORDER BY author_id, id, sum(word_count) - avg(char_length(title)) DESC, COALESCE(array_upper(ARRAY[max(id)],1) * 5,0) DESC;

-- where false queries are router plannable but not fast-path
SELECT *
	FROM articles_hash
	WHERE false;

-- fast-path with false
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and false;

-- fast-path with false
SELECT *
	FROM articles_hash
	WHERE author_id = 1 and 1=0;

SELECT *
	FROM articles_hash
	WHERE null and author_id = 1;

-- we cannot qualify dist_key = X operator Y via
-- fast-path planning
SELECT *
	FROM articles_hash
	WHERE author_id = 1 + 1;

-- where false with immutable function returning false
-- goes through fast-path
SELECT *
	FROM articles_hash a
	WHERE a.author_id = 10 and int4eq(1, 2);

-- partition_column is null clause does not prune out any shards,
-- all shards remain after shard pruning, not router plannable
-- not fast-path router either
SELECT *
	FROM articles_hash a
	WHERE a.author_id is null;

-- partition_column equals to null clause prunes out all shards
-- no shards after shard pruning, router plannable
-- not fast-path router either
SELECT *
	FROM articles_hash a
	WHERE a.author_id = null;

-- union/difference /intersection with where false
-- this query was not originally router plannable, addition of 1=0
-- makes it router plannable but not fast-path
SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash WHERE author_id = 2 and 1=0
) AS combination
ORDER BY id;

-- same with the above, but with WHERE false
SELECT * FROM (
	SELECT * FROM articles_hash WHERE author_id = 1
	UNION
	SELECT * FROM articles_hash WHERE author_id = 2 and 1=0
) AS combination WHERE false
ORDER BY id;

-- window functions with where false
SELECT word_count, rank() OVER (PARTITION BY author_id ORDER BY word_count)
	FROM articles_hash
	WHERE author_id = 1 and 1=0;

-- complex query hitting a single shard and a fast-path
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
-- queries inside transactions can be fast-path router plannable
BEGIN;
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;
END;

-- queries inside read-only transactions can be fast-path router plannable
SET TRANSACTION READ ONLY;
SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;
END;

-- cursor queries are fast-path router plannable
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

-- table creation queries inside can be fast-path router plannable
CREATE TEMP TABLE temp_articles_hash as
	SELECT *
	FROM articles_hash
	WHERE author_id = 1
	ORDER BY id;

-- fast-path router plannable queries may include filter for aggregates
SELECT count(*), count(*) FILTER (WHERE id < 3)
	FROM articles_hash
	WHERE author_id = 1;

-- prepare queries can be router plannable
PREPARE author_1_articles as
	SELECT *
	FROM articles_hash
	WHERE author_id = 1;

EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;

-- parametric prepare queries can be router plannable
PREPARE author_articles(int) as
	SELECT *
	FROM articles_hash
	WHERE author_id = $1;

EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);

EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);

PREPARE author_articles_update(int) AS
	UPDATE articles_hash SET title = 'test' WHERE author_id = $1;

EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);

-- we don't want too many details. though we're omitting
-- "DETAIL:  distribution column value:", we see it acceptable
-- since the query results verifies the correctness
\set VERBOSITY terse

SELECT author_articles_max_id();
SELECT author_articles_max_id();
SELECT author_articles_max_id();
SELECT author_articles_max_id();
SELECT author_articles_max_id();
SELECT author_articles_max_id();

SELECT author_articles_max_id(1);
SELECT author_articles_max_id(1);
SELECT author_articles_max_id(1);
SELECT author_articles_max_id(1);
SELECT author_articles_max_id(1);
SELECT author_articles_max_id(1);

SELECT * FROM author_articles_id_word_count();
SELECT * FROM author_articles_id_word_count();
SELECT * FROM author_articles_id_word_count();
SELECT * FROM author_articles_id_word_count();
SELECT * FROM author_articles_id_word_count();
SELECT * FROM author_articles_id_word_count();

SELECT * FROM author_articles_id_word_count(1);
SELECT * FROM author_articles_id_word_count(1);
SELECT * FROM author_articles_id_word_count(1);
SELECT * FROM author_articles_id_word_count(1);
SELECT * FROM author_articles_id_word_count(1);
SELECT * FROM author_articles_id_word_count(1);

\set VERBOSITY default

-- insert .. select via coordinator could also
-- use fast-path queries
PREPARE insert_sel(int, int) AS
INSERT INTO articles_hash
	SELECT * FROM articles_hash WHERE author_id = $2 AND word_count = $1 OFFSET 0;

EXECUTE insert_sel(1,1);
EXECUTE insert_sel(1,1);
EXECUTE insert_sel(1,1);
EXECUTE insert_sel(1,1);
EXECUTE insert_sel(1,1);
EXECUTE insert_sel(1,1);

-- one final interesting preperad statement
-- where one of the filters is on the target list
PREPARE fast_path_agg_filter(int, int) AS
	SELECT
		count(*) FILTER (WHERE word_count=$1)
	FROM
		articles_hash
	WHERE author_id = $2;

EXECUTE fast_path_agg_filter(1,1);
EXECUTE fast_path_agg_filter(2,2);
EXECUTE fast_path_agg_filter(3,3);
EXECUTE fast_path_agg_filter(4,4);
EXECUTE fast_path_agg_filter(5,5);
EXECUTE fast_path_agg_filter(6,6);

-- views internally become subqueries, so not fast-path router query
SELECT * FROM test_view;

-- materialized views can be created for fast-path router plannable queries
CREATE MATERIALIZED VIEW mv_articles_hash_empty AS
	SELECT * FROM articles_hash WHERE author_id = 1;
SELECT * FROM mv_articles_hash_empty;


SELECT id
	FROM articles_hash
	WHERE author_id = 1;

INSERT INTO articles_hash VALUES (51, 1, 'amateus', 1814), (52, 1, 'second amateus', 2824);

-- verify insert is successfull (not router plannable and executable)
SELECT id
	FROM articles_hash
	WHERE author_id = 1;

SELECT count(*) FROM collections_list WHERE key = 4;
SELECT count(*) FROM collections_list_1 WHERE key = 4;
SELECT count(*) FROM collections_list_2 WHERE key = 4;
UPDATE collections_list SET value = 15 WHERE key = 4;
SELECT count(*) FILTER (where value = 15) FROM collections_list WHERE key = 4;
SELECT count(*) FILTER (where value = 15) FROM collections_list_1 WHERE key = 4;
SELECT count(*) FILTER (where value = 15) FROM collections_list_2 WHERE key = 4;

-- test INSERT using values from generate_series() and repeat() functions
INSERT INTO authors_reference (id, name) VALUES (generate_series(1, 10), repeat('Migjeni', 3));
SELECT * FROM authors_reference ORDER BY 1, 2;
