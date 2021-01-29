CREATE SCHEMA subquery_in_targetlist;
SET search_path TO subquery_in_targetlist, public;

-- simple empty target list
SELECT event_type, (SELECT 1 + 3 + e.value_2)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- simple reference table sublink
SELECT event_type, (SELECT user_id FROM users_reference_table WHERE user_id = 1 AND value_1 = 1)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- duplicate vars
SELECT event_type, (SELECT e.value_2 FROM users_reference_table WHERE user_id = 1 AND value_1 = 1), (SELECT e.value_2)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with aggregate
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery wtth limit
SELECT event_type, (SELECT time FROM users_table WHERE user_id = e.user_id ORDER BY time LIMIT 1)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with group by distribution column
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with group by almost distribution column
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id GROUP BY e.user_id)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery co-located join in outer query
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM users_table u JOIN events_table e USING (user_id)
ORDER BY 1,2 LIMIT 1;

-- correlated subquery non-co-located join in outer query
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM users_table u JOIN events_table e USING (value_2)
ORDER BY 1,2 LIMIT 1;

-- correlated subuqery with non-co-located join
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.value_2)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.value_2 GROUP BY user_id)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with reference table and aggregate
SELECT event_type, (SELECT max(time) FROM users_reference_table WHERE user_id = e.value_2)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with reference table and group by
SELECT event_type, (SELECT max(time) FROM users_reference_table WHERE user_id = e.value_2 GROUP BY user_id)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with reference table join
SELECT (SELECT max(u1.time) FROM users_table u1 JOIN users_reference_table u2 USING (user_id) WHERE u2.user_id = e.user_id GROUP BY user_id), 5
FROM events_table e
GROUP BY 1
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with reference table join and reference table in outer query
SELECT (SELECT max(u1.time) FROM users_table u1 JOIN users_reference_table u2 USING (user_id) WHERE u2.user_id = e.user_id GROUP BY user_id), 5
FROM events_reference_table e
GROUP BY 1
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with non-co-located join in outer query
SELECT event_type, (SELECT max(time) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM users_table u JOIN events_table e USING (value_2)
ORDER BY 1,2 LIMIT 1;

-- aggregate in sublink without join
SELECT event_type, (SELECT max(time) FROM users_table)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- aggregate in ctes in sublink
WITH cte_1 AS (SELECT max(time) FROM users_table)
SELECT event_type, (SELECT * FROM cte_1)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- aggregate in ctes in sublink with limit
WITH cte_1 AS (SELECT max(time) FROM users_table)
SELECT event_type, (SELECT * FROM cte_1 LIMIT 1)
FROM events_table e
ORDER BY 1,2 LIMIT 1;

-- aggregate in ctes in sublink with join
WITH cte_1 AS (SELECT max(time) m FROM users_table)
SELECT count(*), (SELECT * FROM cte_1 c1 join cte_1 c2 using (m))
FROM events_table e
GROUP BY 2
ORDER BY 1,2 LIMIT 1;

-- correlated subquery with cte in outer query
WITH cte_1 AS (SELECT min(user_id) u, max(time) m FROM users_table)
SELECT count(*), (SELECT max(time) FROM users_table WHERE user_id = cte_1.u GROUP BY user_id)
FROM cte_1
GROUP BY 2
ORDER BY 1,2 LIMIT 1;

-- correlated subquery in an aggregate
SELECT sum((SELECT max(value_3) FROM users_table WHERE user_id = e.user_id GROUP BY user_id))
FROM events_table e;

-- correlated subquery outside of an aggregate
SELECT sum(e.user_id) + (SELECT max(value_3) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY e.user_id
ORDER BY 1 LIMIT 3;

-- correlated subquery outside of a non-pushdownable aggregate
SELECT sum(e.user_id) + (SELECT max(value_3) FROM users_reference_table WHERE value_2 = e.value_2 GROUP BY user_id)
FROM events_table e
GROUP BY e.value_2
ORDER BY 1 LIMIT 3;

-- subquery outside of an aggregate
SELECT sum(e.user_id) + (SELECT user_id FROM users_reference_table WHERE user_id = 1 AND value_1 = 1)
FROM events_table e;

-- sublink in a pushdownable window function
SELECT e.user_id, sum((SELECT any_value(value_3) FROM users_reference_table WHERE user_id = e.user_id GROUP BY user_id)) OVER (PARTITION BY e.user_id)
FROM events_table e
ORDER BY 1, 2 LIMIT 3;

-- sublink in a non-pushdownable window function
SELECT e.value_2, sum((SELECT any_value(value_3) FROM users_reference_table WHERE user_id = e.user_id GROUP BY user_id)) OVER (PARTITION BY e.value_2)
FROM events_table e
ORDER BY 1, 2 LIMIT 3;

-- sublink in a group by expression
SELECT e.value_2 + (SELECT any_value(value_3) FROM users_reference_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink
SELECT (SELECT (SELECT user_id + 2) * 2 FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink with outer table reference
SELECT (SELECT (SELECT e.user_id + user_id) FROM users_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink with reference table
SELECT (SELECT (SELECT e.user_id + user_id) FROM users_reference_table WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink with cte
WITH cte_1 AS (SELECT user_id FROM users_table ORDER BY 1 LIMIT 1)
SELECT (SELECT (SELECT e.user_id + user_id) FROM cte_1 WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink
SELECT (SELECT (SELECT e.user_id + user_id) FROM (SELECT 1 AS user_id) s WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink on view
CREATE TEMP VIEW view_1 AS (SELECT user_id, value_2 FROM users_table WHERE user_id = 1 AND value_1 = 1 ORDER BY 1,2);

-- with distribution column group by
SELECT (SELECT value_2 FROM view_1 WHERE user_id = e.user_id GROUP BY user_id, value_2)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- without distribution column group by
SELECT (SELECT value_2 FROM view_1 WHERE user_id = e.user_id GROUP BY value_2)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- without view in the outer query FROM
SELECT (SELECT value_2 FROM view_1 WHERE user_id = e.user_id GROUP BY user_id, value_2)
FROM view_1 e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink in sublink on view
SELECT (SELECT (SELECT e.user_id + user_id) FROM view_1 WHERE user_id = e.user_id GROUP BY user_id)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink on reference table view
CREATE TEMP VIEW view_2 AS (SELECT user_id, value_2 FROM users_reference_table WHERE user_id = 1 AND value_1 = 1);
SELECT (SELECT value_2 FROM view_2 WHERE user_id = e.user_id GROUP BY user_id, value_2)
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- without distributed table view in FROM, reference table view in sublink
SELECT (SELECT value_2 FROM view_2 WHERE user_id = e.user_id GROUP BY user_id, value_2)
FROM view_1 e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- without reference table view in FROM, distributed in sublink
SELECT (SELECT value_2 FROM view_1 WHERE user_id = e.user_id GROUP BY user_id, value_2)
FROM view_2 e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- use view as a type
SELECT (SELECT view_1)
FROM view_1
ORDER BY 1 LIMIT 1;

-- nested correlated sublink
SELECT (SELECT (SELECT user_id))
FROM events_table e
ORDER BY 1 LIMIT 1;

-- sublink with record type
SELECT (SELECT u FROM users_table u WHERE u.user_id = e.user_id AND time = 'Thu Nov 23 09:26:42.145043 2017')
FROM events_table e
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- sublink with anonymous record type
SELECT (SELECT (user_id,value_1) FROM users_table u WHERE u.user_id = e.user_id AND time = 'Thu Nov 23 09:26:42.145043 2017')
FROM events_table e
WHERE user_id < 3
GROUP BY 1
ORDER BY 1 LIMIT 3;

-- complex query using row_to_json
SELECT coalesce(json_agg(root ORDER BY user_id), '[]') AS root
FROM
  (SELECT row_to_json(
                        (SELECT _1_e
                         FROM
                           (SELECT "_0_root.base".user_id AS user_id) AS _1_e)) AS root, user_id
   FROM
     (SELECT DISTINCT user_id FROM public.users_table ORDER BY 1) AS "_0_root.base") AS _2_root ;

SELECT *
FROM
  (SELECT
  	row_to_json((SELECT _1_e FROM (SELECT user_id) AS _1_e)) AS root, user_id
   FROM
     (SELECT DISTINCT user_id FROM public.users_table ORDER BY 1) as bar) AS foo ORDER BY user_id;

-- non-colocated subquery join
SELECT count(*) FROM

	(SELECT event_type, (SELECT e.value_2 FROM users_reference_table WHERE user_id = 1 AND value_1 = 1 AND value_2 = 1), (SELECT e.value_2)
		FROM events_table e) as foo
	JOIN
	(SELECT event_type, (SELECT e.value_2 FROM users_reference_table WHERE user_id = 5 AND value_1 = 1 AND value_2 = 1), (SELECT e.value_2)
		FROM events_table e) as bar
	ON bar.event_type = foo.event_type;

-- subquery in the target list in HAVING should be fine
SELECT
	user_id, count(*)
FROM
	events_table e1
GROUP BY user_id
	HAVING
		count(*) > (SELECT count(*) FROM (SELECT
					  (SELECT sum(user_id)  FROM users_table WHERE user_id = u1.user_id GROUP BY user_id)
					FROM users_table u1
					GROUP BY user_id) as foo) ORDER BY 1 DESC;

-- FROM is empty join tree, sublink can be recursively planned
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table ));

-- FROM is subquery with empty join tree, sublink can be recursively planned
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table ))
FROM (SELECT 1) a;

-- correlated subquery with recurring from clause (prevents recursive planning of outer sublink)
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table) AND value_2 = a)
FROM (SELECT 1 AS a) r;

SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table) AND value_2 = r.user_id)
FROM users_reference_table r;

-- correlated subquery with recurring from clause (prevents recursive planning of inner sublink)
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table WHERE user_id = a))
FROM (SELECT 1 AS a) r;

-- recurring from clause containing a subquery with sublink on distributed table, recursive planning saves the day
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table ))
FROM (SELECT * FROM users_reference_table WHERE user_id IN (SELECT user_id FROM events_table)) r
ORDER BY 1 LIMIT 3;

-- recurring from clause containing a subquery with correlated sublink on distributed table
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table ))
FROM (SELECT * FROM users_reference_table WHERE value_2 IN (SELECT value_2 FROM events_table WHERE events_table.user_id = users_reference_table.user_id)) r
ORDER BY 1 LIMIT 3;

-- recurring from clause with sublink with distributed table in sublink in where
SELECT (SELECT DISTINCT user_id FROM users_reference_table WHERE user_id IN (SELECT user_id FROM users_table) AND user_id < 2), (SELECT 2), 3
FROM users_reference_table r
ORDER BY 1 LIMIT 3;

-- recurring from clause with sublink with distributed table in sublink in target list
SELECT (SELECT 1), (SELECT (SELECT user_id FROM users_table WHERE user_id < 2 GROUP BY user_id)
        FROM users_reference_table WHERE user_id < 2 GROUP BY user_id)
FROM users_reference_table r
ORDER BY 1 LIMIT 3;

-- recurring from clause with correlated sublink with distributed table in sublink in target list
SELECT (SELECT (SELECT user_id FROM users_table WHERE user_id = users_reference_table.user_id GROUP BY user_id)
        FROM users_reference_table WHERE user_id < 2 GROUP BY user_id)
FROM users_reference_table r
ORDER BY 1 LIMIT 3;

-- recurring from clause with correlated sublink with a recurring from clause and a distributed table in sublink
SELECT (SELECT DISTINCT user_id FROM users_reference_table WHERE user_id IN (SELECT user_id FROM users_reference_table) AND value_2 = r.value_2 AND user_id < 2)
FROM users_reference_table r
ORDER BY 1 LIMIT 3;

-- correlated subquery with recursively planned subquery in FROM (outer sublink)
SELECT (SELECT DISTINCT user_id FROM users_table WHERE user_id = (SELECT max(user_id) FROM users_table WHERE user_id = r.user_id))
FROM (SELECT user_id FROM users_table ORDER BY 1 LIMIT 3) r;

-- correlated subquery with recursively planned subquery in FROM (inner sublink)
SELECT (SELECT (SELECT max(user_id) FROM users_table) FROM users_table WHERE user_id = r.user_id)
FROM (SELECT user_id FROM users_table ORDER BY 1 LIMIT 3) r;

-- not meaningful SELECT FOR UPDATE query that should fail
SELECT count(*) FROM (SELECT
  (SELECT user_id FROM users_table WHERE user_id = u1.user_id FOR UPDATE)
FROM users_table u1
GROUP BY user_id) as foo;

DROP SCHEMA subquery_in_targetlist CASCADE;
