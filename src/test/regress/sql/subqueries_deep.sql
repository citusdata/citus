-- ===================================================================
-- test recursive planning functionality with subqueries and CTEs
-- ===================================================================
CREATE SCHEMA subquery_deep;
SET search_path TO subquery_and_ctes, public;

SET client_min_messages TO DEBUG1;

-- subquery in FROM -> FROM -> FROM should be replaced due to OFFSET
-- one level up subquery should be replaced due to GROUP BY on non partition key
-- one level up subquery should be replaced due to LIMUT
SELECT 
	DISTINCT user_id 
FROM 
	(
		SELECT users_table.user_id FROM users_table, 
							(
								SELECT 
									avg(event_type) as avg_val
								FROM
									(SELECT event_type, users_table.user_id FROM users_table, 
															(SELECT user_id, event_type FROM events_table WHERE value_2 < 3 OFFSET 3) as foo
															WHERE foo.user_id = users_table.user_id
															) bar, users_table WHERE bar.user_id = users_table.user_id GROUP BY users_table.value_1

							) as baz
									WHERE baz.avg_val < users_table.user_id
								    LIMIT 3

	) as sub1
	ORDER BY 1 DESC;

-- subquery in FROM -> FROM -> WHERE ->  WHERE should be replaced due to CTE
-- subquery in FROM -> FROM -> WHERE should be replaced due to LIMIT
-- one level above should be replaced due to DISTINCT on non-partition key
-- one level above should be replaced due to GROUP BY on non-partition key
SELECT event, array_length(events_table, 1)
FROM (
  SELECT event, array_agg(t.user_id) AS events_table
  FROM (
    SELECT 
    	DISTINCT ON(e.event_type::text) e.event_type::text as event, e.time, e.user_id
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
				AND EXISTS (WITH cte AS (SELECT count(*) FROM users_table) SELECT * FROM cte)
				LIMIT 5
    		)
  ) t, users_table WHERE users_table.value_1 = t.event::int
  GROUP BY event
) q
ORDER BY 2 DESC, 1;

-- this test probably doesn't add too much value, 
-- but recurse 6 times for fun

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

-- same query happening in the subqueries in WHERE

-- this test probably doesn't add too much value, 
-- but recurse 6 times for fun
SELECT 
	* 
FROM 
	users_table 
WHERE user_id IN (
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
				) as bar);


SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_deep CASCADE;
SET search_path TO public;