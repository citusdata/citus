-- ===================================================================
-- test recursive planning functionality with complex target entries
-- and some utilities
-- ===================================================================
CREATE SCHEMA subquery_complex;
SET search_path TO subquery_complex, public;

SET client_min_messages TO DEBUG1;

-- the logs are enabled and it sometimes
-- lead to flaky outputs when jit enabled
SET jit_above_cost TO -1;

-- COUNT DISTINCT at the top level query
SELECT
  event_type, count(distinct value_2)
FROM
  events_table
WHERE
  user_id IN (SELECT user_id FROM users_table GROUP BY user_id ORDER BY count(*) DESC LIMIT 20)
GROUP BY
  event_type
ORDER BY 1 DESC, 2 DESC
LIMIT 3;

-- column renaming in a subquery
SELECT *
FROM
	(
	SELECT user_id, value_1, value_2 FROM users_table OFFSET 0
	) as foo(x, y)
ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 5;

-- aggregate distinct in the subqueries
	-- avg distinct on partition key
	-- count distinct on partition key
	-- count distinct on non-partition key
	-- sum distinct on non-partition key when group by is partition key
	-- and the final query is real-time query
SELECT
   DISTINCT ON (avg) avg, cnt_1, cnt_2, sum
FROM
    (
    	SELECT avg(distinct user_id) as avg FROM users_table ORDER BY 1 DESC LIMIT 3
     ) as foo,
    (
    	SELECT count(distinct user_id) as cnt_1 FROM users_table ORDER BY 1 DESC LIMIT 3
    ) as bar,
	(
    	SELECT count(distinct value_2) as cnt_2 FROM users_table ORDER BY 1 DESC LIMIT 4
    ) as baz,
	(
		SELECT user_id, sum(distinct value_2) as sum FROM users_table GROUP BY user_id ORDER BY 1 DESC LIMIT 4
    ) as bat, events_table
    WHERE foo.avg != bar.cnt_1 AND baz.cnt_2 = events_table.event_type
    ORDER BY 1 DESC;

-- Aggregate type conversions inside the subqueries
SELECT
   *
FROM
    (
    	SELECT
    		min(user_id) * 2, max(user_id) / 2, sum(user_id), count(user_id)::float, avg(user_id)::bigint
    	FROM
    		users_table
    	ORDER BY 1 DESC
    	LIMIT 3
    ) as foo,
    (
    	   SELECT
    		min(value_3) * 2, max(value_3) / 2, sum(value_3), count(value_3), avg(value_3)
    	FROM
    		users_table
    	ORDER BY 1 DESC
    	LIMIT 3
    ) as bar,
	(
    	SELECT
    		min(time), max(time), count(time),
    		count(*) FILTER (WHERE user_id = 3) as cnt_with_filter,
    		count(*) FILTER (WHERE user_id::text LIKE '%3%') as cnt_with_filter_2
    	FROM
    		users_table
    	ORDER BY 1 DESC
    	LIMIT 3
    ) as baz
    ORDER BY 1 DESC;

-- Expressions inside the aggregates
-- parts of the query is inspired by TPCH queries
SELECT
   DISTINCT ON (avg) avg, cnt_1, cnt_2, cnt_3, sum_1,l_year, pos, count_pay
FROM
    (
    	SELECT avg(user_id * (5.0 / (value_1 + 0.1))) as avg FROM users_table ORDER BY 1 DESC LIMIT 3
     ) as foo,
    (
    	SELECT sum(user_id * (5.0 / (value_1 + value_2 + 0.1)) * value_3) as cnt_1 FROM users_table ORDER BY 1 DESC LIMIT 3
    ) as bar,
	(
    	SELECT
    		avg(case
            	when user_id > 4
            	then value_1
        	end) as cnt_2,
    		avg(case
            	when user_id > 500
            	then value_1
        	end) as cnt_3,
    		sum(case
				when value_1 = 1
			 	OR value_2 = 1
				then 1
				else 0
			end) as sum_1,
			extract(year FROM max(time)) as l_year,
			strpos(max(user_id)::text, '1') as pos
         FROM
         		users_table
         ORDER BY
         	1 DESC
         LIMIT 4
    ) as baz,
	(
		SELECT  COALESCE(value_3, 20) AS count_pay FROM users_table ORDER BY 1 OFFSET 20 LIMIT 5
	) as tar,
    events_table
    WHERE foo.avg != bar.cnt_1 AND baz.cnt_2 != events_table.event_type
    ORDER BY 1 DESC;

-- Multiple columns in GROUP BYs
-- foo needs to be recursively planned, bar can be pushded down
SELECT
   DISTINCT ON (avg) avg, avg2
FROM
    (
    	SELECT avg(value_3) as avg FROM users_table GROUP BY value_1, value_2
     ) as foo,
    (
    	SELECT avg(value_3) as avg2 FROM users_table GROUP BY value_1, value_2, user_id
     ) as bar
    WHERE foo.avg = bar.avg2
    ORDER BY 1 DESC, 2 DESC
    LIMIT 3;

-- HAVING and ORDER BY tests
SELECT a.user_id, b.value_2, c.avg
FROM (
         SELECT
         	user_id
    	 FROM
         	users_table
         WHERE
           	(value_1 > 2)
          GROUP BY
      			user_id
           HAVING
      		count(distinct value_1) > 2
      	   ORDER BY 1 DESC
      	   LIMIT 3
        ) as a,
		(
         SELECT
         	value_2
    	 FROM
         	users_table
         WHERE
           	(value_1 > 2)
          GROUP BY
      			value_2
           HAVING
      		count(distinct value_1) > 2
      	   ORDER BY 1 DESC
      	   LIMIT 3
        ) as b,
       	(
         SELECT
         	avg(user_id) as avg
    	 FROM
         	users_table
         WHERE
           	(value_1 > 2)
          GROUP BY
      			value_2
           HAVING
      		sum(value_1) > 10
      	   ORDER BY (sum(value_3) - avg(value_1) - COALESCE(array_upper(ARRAY[max(user_id)],1) * 5,0)) DESC
      	   LIMIT 3
        ) as c

        WHERE b.value_2 != a.user_id
        ORDER BY 3 DESC, 2 DESC, 1 DESC
        LIMIT 5;

-- zero shard subquery joined with a regular one
SELECT
   bar.user_id
FROM
    (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo,
    (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND false AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as bar
    WHERE foo.user_id > bar.user_id
    ORDER BY 1 DESC;

-- window functions tests, both is recursively planned
SELECT * FROM
(
	SELECT
	   user_id, time, rnk
	FROM
	(
		SELECT * FROM (
	  SELECT
	    *, rank() OVER my_win as rnk
	  FROM
	    events_table
	    WINDOW my_win AS (PARTITION BY user_id ORDER BY time DESC)
	    ORDER BY rnk DESC
		) as foo_inner
     ORDER BY user_id DESC
	   LIMIT 4
	) as foo
	ORDER BY
	  3 DESC, 1 DESC, 2 DESC
) foo,
(
	SELECT
	   user_id, time, rnk
	FROM
	(
	  SELECT
	    *, rank() OVER my_win as rnk
	  FROM
	    events_table
	  WHERE
	   	user_id = 3
	  WINDOW my_win AS (PARTITION BY event_type ORDER BY time DESC)

	) as foo
	ORDER BY
	  3 DESC, 1 DESC, 2 DESC
) bar WHERE foo.user_id = bar.user_id
ORDER BY foo.rnk DESC, foo.time DESC, bar.time LIMIT 5;

-- cursor test
BEGIN;

	DECLARE recursive_subquery CURSOR FOR
	SELECT
	  event_type, count(distinct value_2)
	FROM
	  events_table
	WHERE
	  user_id IN (SELECT user_id FROM users_table GROUP BY user_id ORDER BY count(*) DESC LIMIT 20)
	GROUP BY
	  event_type
	ORDER BY 1 DESC, 2 DESC
	LIMIT 3;

	FETCH 1 FROM recursive_subquery;
	FETCH 1 FROM recursive_subquery;
	FETCH 1 FROM recursive_subquery;
	FETCH 1 FROM recursive_subquery;
COMMIT;

-- cursor test with FETCH ALL
BEGIN;

	DECLARE recursive_subquery CURSOR FOR
	SELECT
	  event_type, count(distinct value_2)
	FROM
	  events_table
	WHERE
	  user_id IN (SELECT user_id FROM users_table GROUP BY user_id ORDER BY count(*) DESC LIMIT 20)
	GROUP BY
	  event_type
	ORDER BY 1 DESC, 2 DESC
	LIMIT 3;

	FETCH ALL FROM recursive_subquery;
	FETCH ALL FROM recursive_subquery;
COMMIT;

SET client_min_messages TO DEFAULT;

CREATE TABLE items (key text primary key, value text not null, t timestamp);
SELECT create_distributed_table('items','key');
INSERT INTO items VALUES ('key-1','value-2', '2020-01-01 00:00');
INSERT INTO items VALUES ('key-2','value-1', '2020-02-02 00:00');

CREATE TABLE other_items (key text primary key, value text not null);
SELECT create_distributed_table('other_items','key');
INSERT INTO other_items VALUES ('key-1','value-2');

-- LEFT JOINs are wrapped into a subquery under the covers, which causes GROUP BY
-- to be separated from the LEFT JOIN. If the GROUP BY is on a primary key we can
-- normally use any column even ones that are not in the GROUP BY, but not when
-- it is in the outer query. In that case, we use the any_value aggregate.
SELECT key, a.value, count(b.value), t
FROM items a LEFT JOIN other_items b USING (key)
GROUP BY key HAVING a.value != 'value-2' ORDER BY count(b.value), a.value LIMIT 5;

SELECT key, a.value, count(b.value), t
FROM items a LEFT JOIN other_items b USING (key)
GROUP BY key, t HAVING a.value != 'value-2' ORDER BY count(b.value), a.value LIMIT 5;

-- make sure the same logic works for regular joins
SELECT key, a.value, count(b.value), t
FROM items a JOIN other_items b USING (key)
GROUP BY key HAVING a.value = 'value-2' ORDER BY count(b.value), a.value LIMIT 5;

-- subqueries also trigger wrapping
SELECT key, a.value, count(b.value), t
FROM items a JOIN (SELECT key, value, random() FROM other_items) b USING (key)
GROUP BY key ORDER BY 3, 2, 1;

-- pushdownable window functions also trigger wrapping
SELECT a.key, a.value, count(a.value) OVER (PARTITION BY a.key)
FROM items a JOIN other_items b ON (a.key = b.key)
GROUP BY a.key ORDER BY 3, 2, 1;

-- left join with non-pushdownable window functions
SELECT a.key, a.value, count(a.value) OVER ()
FROM items a LEFT JOIN other_items b ON (a.key = b.key)
GROUP BY a.key ORDER BY 3, 2, 1;

-- function joins (actually with read_intermediate_results) also trigger wrapping
SELECT key, a.value, sum(b)
FROM items a JOIN generate_series(1,10) b ON (a.key = 'key-'||b)
GROUP BY key ORDER BY 3, 2, 1;

DROP SCHEMA subquery_complex CASCADE;
