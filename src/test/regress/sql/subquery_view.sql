-- ===================================================================
-- test recursive planning functionality on views
-- ===================================================================

CREATE SCHEMA subquery_view;
SET search_path TO subquery_view, public;


CREATE TABLE users_table_local AS SELECT * FROM users_table;
CREATE TABLE events_table_local AS SELECT * FROM events_table;

SET client_min_messages TO DEBUG1;

CREATE VIEW view_without_subquery AS
SELECT 
    	DISTINCT users_table.value_1 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC;

SELECT 
	* 
FROM 
	view_without_subquery 
ORDER BY 1 DESC LIMIT 5;

CREATE VIEW view_without_subquery_second AS
SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC
     LIMIT 5;
SELECT 
	* 
FROM 
	view_without_subquery_second 
ORDER BY 1;

-- subqueries in FROM clause with LIMIT should be recursively planned
CREATE VIEW subquery_limit AS
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

SELECT * FROM subquery_limit ORDER BY 1 DESC;

-- subqueries in FROM clause with GROUP BY non-distribution column should be recursively planned
CREATE VIEW subquery_non_p_key_group_by AS 
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

SELECT * FROM subquery_non_p_key_group_by ORDER BY 1 DESC;



CREATE VIEW final_query_router AS 
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

SELECT * FROM final_query_router ORDER BY 1;

CREATE VIEW final_query_realtime AS
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

SELECT 
	DISTINCT ON (users_table.value_2) users_table.value_2, time, value_3 
FROM 
	final_query_realtime, users_table
WHERE 
	users_table.user_id = final_query_realtime.user_id
ORDER BY 1 DESC, 2 DESC, 3 DESC
LIMIT 3;


CREATE VIEW subquery_in_where AS
SELECT DISTINCT user_id
FROM users_table
WHERE 
	user_id IN (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5);


SELECT 
	* 
FROM 
	subquery_in_where
ORDER BY 1 DESC;


-- subquery in FROM -> FROM -> WHERE should be replaced due to LIMIT
CREATE VIEW subquery_from_from_where AS 
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
) q;

SELECT 
	* 
FROM 
	subquery_from_from_where
ORDER BY 
2 DESC, 1;


-- subquery in FROM -> FROM -> FROM should be replaced if 
-- it contains onle local tables
CREATE VIEW subquery_from_from_where_local_table AS 
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
															(SELECT user_id, event_type FROM events_table_local WHERE value_2 < 3 OFFSET 3) as foo
															WHERE foo.user_id = users_table.user_id
															) bar

							) as baz
									WHERE baz.user_id = users_table.user_id

	) as sub1;

SELECT 
	* 
FROM 
	subquery_from_from_where
ORDER BY 1 DESC 
	LIMIT 3;

SET citus.enable_repartition_joins to ON;

CREATE VIEW repartition_view AS
SELECT 
	count(*) 
FROM
(
	SELECT DISTINCT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND users_table.user_id < 2
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.value_2 = bar.user_id; 

SELECT 
	* 
FROM 
	repartition_view;

CREATE VIEW all_executors_view AS
SELECT 
	count(*) 
FROM
(
	SELECT value_2 FROM users_table WHERE user_id = 15 OFFSET 0
) as foo, 
(
	SELECT user_id FROM users_table OFFSET 0
) as bar,
(
	SELECT DISTINCT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND users_table.user_id < 2
) baz,
(
	SELECT user_id FROM users_table_local WHERE user_id = 2
) baw
WHERE foo.value_2 = bar.user_id AND baz.value_2 = bar.user_id AND bar.user_id = baw.user_id; 

SELECT 
	* 
FROM 
	all_executors_view;

SET citus.enable_repartition_joins to OFF;


-- the same query, but this time the CTEs also live inside a subquery
CREATE VIEW subquery_and_ctes AS 
SELECT 
	* 
FROM 
(

	WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT 
			user_id
		FROM 
			events_table, 
			(SELECT DISTINCT value_2 FROM users_table OFFSET 0) as foo
		WHERE 
			events_table.user_id = foo.value_2 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*)  as cnt
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id

) as foo, users_table WHERE foo.cnt > users_table.value_2;

SELECT * FROM subquery_and_ctes
ORDER BY 3 DESC, 1 DESC, 2 DESC, 4 DESC
LIMIT 5;


CREATE VIEW subquery_and_ctes_second AS 
SELECT time, event_type, value_2, value_3 FROM 
(
	WITH cte AS (
		WITH local_cte AS (
			SELECT * FROM users_table_local
		),
		dist_cte AS (
			SELECT user_id FROM events_table
		)
		SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
	)
	SELECT DISTINCT cte.user_id
	FROM users_table, cte
	WHERE 
		users_table.user_id = cte.user_id AND
		users_table.user_id IN 
			(WITH cte_in_where AS (SELECT DISTINCT value_2 FROM users_table WHERE value_1 >= 1 AND value_1 <= 20 ORDER BY 1 LIMIT 5) SELECT * FROM cte_in_where)
	    ORDER BY 1 DESC
	    ) as foo, 
			events_table 
		WHERE 
			foo.user_id = events_table.value_2;


SELECT * FROM subquery_and_ctes_second
ORDER BY 3 DESC, 2 DESC, 1 DESC 
LIMIT 5;

CREATE VIEW deep_subquery AS
SELECT count(*)
FROM
(
	SELECT avg(min) FROM 
	(
		SELECT min(users_table.value_1) FROM
		(
			SELECT avg(event_type) as avg_ev_type FROM 
			(
				SELECT 
					max(value_1) as mx_val_1 
					FROM (
							SELECT 
								avg(event_type) as avg
							FROM
							(
								SELECT 
									cnt 
								FROM 
									(SELECT count(*) as cnt, value_2 FROM users_table GROUP BY value_2) as level_1, users_table
								WHERE 
									users_table.user_id = level_1.cnt
							) as level_2, events_table
							WHERE events_table.user_id = level_2.cnt 
							GROUP BY level_2.cnt
						) as level_3, users_table
					WHERE user_id = level_3.avg
					GROUP BY level_3.avg
					) as level_4, events_table
				WHERE level_4.mx_val_1 = events_table.user_id
				GROUP BY level_4.mx_val_1
				) as level_5, users_table
				WHERE 
					level_5.avg_ev_type = users_table.user_id
				GROUP BY 
					level_5.avg_ev_type
		) as level_6, users_table WHERE users_table.user_id = level_6.min
	GROUP BY users_table.value_1
	) as bar;

SELECT 
	* 
FROM 
	deep_subquery;


CREATE VIEW result_of_view_is_also_recursively_planned AS
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
SELECT 
	*
FROM
	(SELECT 
		* 
	 FROM
		result_of_view_is_also_recursively_planned, events_table 
	 WHERE 
	 	events_table.value_2 = result_of_view_is_also_recursively_planned.user_id
	  ORDER BY time DESC
	  LIMIT 5
	  OFFSET 4

	  ) as foo
ORDER BY time DESC LIMIT 5;

SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_view CASCADE;
SET search_path TO public;