-- ===================================================================
-- test recursive planning functionality
-- ===================================================================
SET client_min_messages TO DEBUG1;

-- the subquery is safe to pushdown, should not
-- recursively plan
SELECT
	user_id, value_1
FROM
	(SELECT user_id, value_1 FROM users_table) as foo
ORDER BY 1 DESC, 2 DESC LIMIT 3;

-- the subquery is safe to pushdown, should not
-- recursively plan
SELECT
	sum(sel_val_1), sum(sel_val_2)
FROM
	(SELECT max(value_1) as  sel_val_1, min(value_2) as sel_val_2 FROM users_table GROUP BY user_id) as foo;

-- the subquery is safe to pushdown, should not
-- recursively plan
SELECT
	min(user_id), max(value_1)
FROM
	(SELECT user_id, value_1 FROM users_table) as foo;

-- the subquery is safe to pushdown, should not
-- recursively plan
SELECT
	min(user_id)
FROM
	(SELECT user_id, value_1 FROM users_table GROUP BY user_id, value_1) as bar;

-- the subquery is safe to pushdown, should not
-- recursively plan
SELECT
	min(user_id), sum(max_value_1)
FROM
	(SELECT user_id, max(value_1) as max_value_1 FROM users_table GROUP BY user_id) as bar;

-- subqueries in FROM clause with LIMIT should be recursively planned
SELECT
   user_id
FROM
    (SELECT
    	DISTINCT users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo
    ORDER BY 1 DESC;


-- subqueries in FROM clause with DISTINCT on non-partition key
-- should be recursively planned
SELECT
   *
FROM
    (SELECT
    	DISTINCT users_table.value_1
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     ORDER BY 1
     ) as foo
     ORDER BY 1 DESC;

-- subqueries in FROM clause with GROUP BY on non-partition key
-- should be recursively planned
SELECT
   *
FROM
    (SELECT
    	users_table.value_2, avg(value_1)
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     GROUP BY users_table.value_2
     ORDER BY 1 DESC
     ) as foo
    ORDER BY 2 DESC, 1;

-- subqueries with only generate_series
SELECT
   *
FROM
    (SELECT
        events_table.value_2
     FROM
        events_table
    WHERE
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC
     LIMIT 5
     ) as foo,
    (
        SELECT i FROM generate_series(0, 100) i
    ) as bar
    WHERE foo.value_2 = bar.i
    ORDER BY 2 DESC, 1;

-- subquery with aggregates without GROUP BY
SELECT
   *
FROM
    (SELECT
        count(*)
     FROM
        events_table
    WHERE
     event_type IN (1,2,3,4)
     ) as foo;

-- subquery having without GROUP BY
SELECT
   *
FROM
    (SELECT
          SUM(events_table.user_id)
     FROM
        events_table
    WHERE
     event_type IN (1,2,3,4)
    HAVING
        MIN(value_2) > 2
     ) as foo;

-- multiple subqueries in FROM clause should be replaced
-- and the final query is router query
SELECT
   *
FROM
    (SELECT
    	users_table.value_2
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     GROUP BY users_table.value_2
     ORDER BY 1 DESC
     ) as foo,
	(SELECT
    	users_table.value_3
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (5,6,7,8)
     GROUP BY users_table.value_3
     ORDER BY 1 DESC
     ) as bar
	WHERE foo.value_2 = bar.value_3
	ORDER BY 2 DESC, 1;

-- same query with alias in the subquery
SELECT
   DISTINCT ON (citus) citus, postgres, citus + 1 as c1, postgres-1 as p1
FROM
    (SELECT
        users_table.value_2
     FROM
        users_table, events_table
     WHERE
        users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     GROUP BY users_table.value_2
     ORDER BY 1 DESC
     ) as foo(postgres),
    (SELECT
        users_table.user_id
     FROM
        users_table, events_table
     WHERE
        users_table.user_id = events_table.user_id AND
     event_type IN (5,6,7,8)
     ORDER BY 1 DESC
     ) as bar (citus)
    WHERE foo.postgres = bar.citus
    ORDER BY 1 DESC, 2 DESC
    LIMIT 3;

-- foo is replaced
-- and the final query is real-time
SELECT
   *
FROM
    (SELECT
    	users_table.value_2
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (1,2,3,4)
     GROUP BY users_table.value_2
     ORDER BY 1 DESC
     ) as foo,
	(SELECT
    	users_table.user_id
     FROM
     	users_table, events_table
     WHERE
     	users_table.user_id = events_table.user_id AND
     event_type IN (5,6,7,8)
     ORDER BY 1 DESC
     ) as bar
	WHERE foo.value_2 = bar.user_id
	ORDER BY 2 DESC, 1 DESC
	LIMIT 3;

-- subqueries in WHERE should be replaced
SELECT DISTINCT user_id
FROM users_table
WHERE
	user_id IN (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5)
    ORDER BY 1 DESC;

-- subquery in FROM -> FROM -> FROM should be replaced due to OFFSET
SELECT
	DISTINCT user_id
FROM
	(
		SELECT users_table.user_id FROM users_table,
							(
								SELECT
									event_type, user_id
								FROM
									(SELECT event_type, users_table.user_id FROM users_table,
															(SELECT user_id, event_type FROM events_table WHERE value_2 < 3 OFFSET 3) as foo
															WHERE foo.user_id = users_table.user_id
															) bar

							) as baz
									WHERE baz.user_id = users_table.user_id

	) as sub1
	ORDER BY 1 DESC
	LIMIT 3;


-- subquery in FROM -> FROM -> WHERE should be replaced due to LIMIT
SELECT user_id, array_length(events_table, 1)
FROM (
  SELECT user_id, array_agg(event ORDER BY time) AS events_table
  FROM (
    SELECT
    	u.user_id, e.event_type::text AS event, e.time
    FROM
    	users_table AS u,
        events_table AS e
    WHERE u.user_id = e.user_id AND
    		u.user_id IN
    		(
    			SELECT
    				user_id
    			FROM
    				users_table
    			WHERE value_2 >= 5
			    AND  EXISTS (SELECT user_id FROM events_table WHERE event_type > 1 AND event_type <= 3 AND value_3 > 1 AND user_id = users_table.user_id)
				AND  NOT EXISTS (SELECT user_id FROM events_table WHERE event_type > 3 AND event_type <= 4  AND value_3 > 1 AND user_id = users_table.user_id)
				LIMIT 5
    		)
  ) t
  GROUP BY user_id
) q
ORDER BY 2 DESC, 1;


-- subquery (i.e., subquery_2) in WHERE->FROM should be replaced due to LIMIT
SELECT
	user_id
FROM
	users_table
WHERE
 user_id IN
(
	SELECT
	  user_id
	 FROM (
	  SELECT
	 	 subquery_1.user_id, count_pay
	  FROM
	  (
	    (SELECT
	      users_table.user_id,
	      'action=>1' AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 1 AND
	      users_table.user_id <= 3 AND
	      events_table.event_type > 1 AND events_table.event_type < 3
	      )
	    UNION
	    (SELECT
	      users_table.user_id,
	      'action=>2' AS event,
	      events_table.time
	    FROM
	      users_table,
	      events_table
	    WHERE
	      users_table.user_id = events_table.user_id AND
	      users_table.user_id >= 1 AND
	      users_table.user_id <= 3 AND
	      events_table.event_type > 2 AND events_table.event_type < 4
	    )
	  ) AS subquery_1
	  LEFT JOIN
	    (SELECT
	       user_id,
	      COUNT(*) AS count_pay
	    FROM
	      users_table
	    WHERE
	      user_id >= 1 AND
	      user_id <= 3 AND
	      users_table.value_1 > 3 AND users_table.value_1 < 5
	    GROUP BY
	      user_id
	    HAVING
	      COUNT(*) > 1
	      LIMIT 10

	      ) AS subquery_2
	  ON
	    subquery_1.user_id = subquery_2.user_id
	  GROUP BY
	    subquery_1.user_id,
	    count_pay) AS subquery_top
	GROUP BY
	  count_pay, user_id
)
GROUP BY user_id
HAVING count(*) > 1 AND sum(value_2) > 29
ORDER BY 1;

-- we support queries with recurring tuples in the FROM
-- clause and subquery in WHERE clause
SELECT
	*
FROM
	(
		SELECT
			users_table.user_id
		FROM
			users_table, (SELECT user_id FROM events_table) as evs
		WHERE users_table.user_id = evs.user_id
		ORDER BY 1
		LIMIT 5
	) as foo WHERE user_id IN (SELECT count(*) FROM users_table GROUP BY user_id);

CREATE TABLE dist(id int, value int);
SELECT create_distributed_table('dist','id');
INSERT INTO dist SELECT i, i FROM generate_series(0,100) i;
CREATE TABLE ref(id int);
SELECT create_reference_table('ref');
INSERT INTO ref SELECT i FROM generate_series(50,150) i;
CREATE TABLE local(id int);
INSERT INTO local SELECT i FROM generate_series(50,150) i;

-- planner recursively plans local table in local-dist join and then the whole query is routed
SELECT COUNT(*) FROM dist JOIN local USING(id)
WHERE
	dist.id IN (SELECT id FROM dist WHERE id = 55) AND
    dist.id = 55 AND
    dist.value IN (SELECT value FROM dist WHERE id = 55);

-- subquery in WHERE clause should be recursively planned after planner recursively plans recurring full join
SELECT COUNT(*) FROM ref FULL JOIN dist USING (id)
WHERE
	dist.id IN (SELECT id FROM dist GROUP BY id);

-- subqueries in WHERE clause should be recursively planned after planner recursively plans full outer join
SELECT COUNT(*) FROM dist FULL JOIN ref USING(id)
WHERE
	dist.id IN (SELECT id FROM dist WHERE id > 5) AND
    dist.value IN (SELECT value FROM dist WHERE id > 15);

-- sublinks in the targetlist are not supported
SELECT (SELECT id FROM dist WHERE dist.id > d1.id GROUP BY id) FROM ref FULL JOIN dist d1 USING (id);

SET client_min_messages TO WARNING;
DROP TABLE dist;
DROP TABLE ref;
DROP TABLE local;
