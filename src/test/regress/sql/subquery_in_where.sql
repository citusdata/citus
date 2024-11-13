-- ===================================================================
-- test recursive planning functionality with subqueries in WHERE
-- ===================================================================
CREATE SCHEMA subquery_in_where;
SET search_path TO subquery_in_where, public;

SET client_min_messages TO DEBUG1;

--CTEs can be used as a recurring tuple with subqueries in WHERE
WITH event_id
     AS MATERIALIZED (SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type
         FROM   events_table)
SELECT Count(*)
FROM   event_id
WHERE  events_user_id IN (SELECT user_id
                          FROM   users_table);

--Correlated subqueries can not be used in WHERE clause
WITH event_id
     AS(SELECT user_id AS events_user_id,
                time    AS events_time,
                event_type
         FROM   events_table)
SELECT Count(*)
FROM   event_id
WHERE  (events_user_id, random()) IN (SELECT user_id, 1
                          FROM   users_table
                          WHERE  users_table.time = events_time);

-- Recurring tuples as empty join tree
SELECT *
FROM   (SELECT 1 AS id, 2 AS value_1, 3 AS value_3
		UNION ALL SELECT 2 as id, 3 as value_1, 4 as value_3) AS tt1
WHERE  id IN (SELECT user_id
              FROM   events_table);

-- Recurring tuples in from clause as CTE and SET operation in WHERE clause
SELECT Count(*)
FROM   (WITH event_id AS
       (SELECT user_id AS events_user_id, time AS events_time, event_type
        FROM events_table)
       SELECT events_user_id, events_time, event_type
	   FROM event_id
	   ORDER BY 1,2,3
	   LIMIT 10) AS sub_table
WHERE  events_user_id IN (
       (SELECT user_id
        FROM users_table
        ORDER BY 1
        LIMIT 10)
		UNION ALL
       (SELECT value_1
        FROM users_table
        ORDER BY 1
        limit 10));

-- Recurring tuples in from clause as SET operation on recursively plannable
-- queries and CTE in WHERE clause
SELECT
	*
FROM
	(
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id ASC
		LIMIT
			10
		)
		UNION ALL
		(SELECT
			value_1
		FROM
			users_table
		ORDER BY
			value_1 ASC
		LIMIT
			10
		)
	) as SUB_TABLE
WHERE
	user_id
IN
	(
	WITH event_id AS (
		SELECT
			user_id as events_user_id, time as events_time, event_type
		FROM
			events_table
	)
	SELECT
		events_user_id
	FROM
		event_id
	ORDER BY
		events_user_id
	LIMIT
		10
	);

-- Complex target list in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY
		1,2
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
<=ANY (
	SELECT
		max(abs(user_id * 1) + mod(user_id, 3)) as val_1
	FROM
		users_table
	GROUP BY
		user_id
);

-- DISTINCT clause in WHERE
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
IN (
	SELECT
		distinct user_id
	FROM
		users_table
	GROUP BY
		user_id
);

-- AND in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY
		1,2,3
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
>=ANY (
	SELECT
		min(user_id)
	FROM
		users_table
	GROUP BY
		user_id
)
AND
	events_user_id
<=ANY (
	SELECT
		max(user_id)
	FROM
		users_table
	GROUP BY
		user_id
);

-- AND in WHERE clause, part of the AND is pushdownable other is not
SELECT
	COUNT(*)
FROM
	(SELECT
		user_id as events_user_id, time as events_time, event_type
	FROM
		events_table
	ORDER BY
		1,2,3
	LIMIT
		10
	) as SUB_TABLE
WHERE
	events_user_id
>=ANY (
	SELECT
		min(user_id)
	FROM
		users_table
	GROUP BY
		user_id
)
AND
	events_user_id
<=ANY (
	SELECT
		max(value_2)
	FROM
		users_table
	GROUP BY
		user_id
);

-- Planning subqueries in WHERE clause in CTE recursively
WITH cte AS (
	SELECT
		*
	FROM
		(SELECT
			*
		FROM
			users_table
		ORDER BY
			user_id ASC,
			value_2 DESC
		LIMIT
			10
		) as sub_table
	WHERE
		user_id
	IN
		(SELECT
			value_2
		FROM
			events_table
		)
)
SELECT
	COUNT(*)
FROM
	cte;

-- Planing subquery in WHERE clause in FROM clause of a subquery recursively
SELECT
	COUNT(*)
FROM
	(SELECT
		*
	FROM
		(SELECT
			*
		FROM
			users_table
		ORDER BY
			user_id ASC,
			value_2 DESC
		LIMIT
			10
		) as sub_table_1
	WHERE
		user_id
	IN
		(SELECT
			value_2
		FROM
			events_table
		)
	) as sub_table_2;

-- Recurring table in the FROM clause of a subquery in the FROM clause
-- Recurring table is created by joining a two recurrign table
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE
	user_id
>ANY
	(SELECT
		min(user_id)
	FROM
		events_table
	GROUP BY
		user_id);

-- Same example with the above query, but now check the rows with EXISTS
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE EXISTS
	(SELECT
		1,2
	FROM
		events_table
	WHERE
		events_table.value_2 = events_table.user_id);

-- Same query with the above one, yet now we check the row's NON-existence
-- by NOT EXISTS. Note that, max value_2 of events_table is 5
SELECT
	SUM(user_id)
FROM
	(SELECT
		*
	FROM
		(SELECT
			user_id
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT 10) as t1
		INNER JOIN
		(SELECT
			user_id as user_id_2
		FROM
			users_table
		ORDER BY
			user_id
		LIMIT
			10) as t2
	ON
		t1.user_id = t2.user_id_2
	WHERE
		t1.user_id
	IN
		(SELECT
			value_2
		FROM
			events_table)
	) as t3
WHERE NOT EXISTS
	(SELECT
		1,2
	FROM
		events_table
	WHERE
		events_table.value_2 = events_table.user_id + 6);

-- Check the existence of row by comparing it with the result of subquery in
-- WHERE clause. Note that subquery is planned recursively since there is no
-- distributed table in the from
SELECT
	*
FROM
	(SELECT
		user_id, value_1
	FROM
		users_table
	ORDER BY
		user_id ASC,
		value_1 ASC
	LIMIT 10) as t3
WHERE row(user_id, value_1) =
	(SELECT
		min(user_id) + 1, min(user_id) + 1
	FROM
		events_table);

-- Recursively plan subquery in WHERE clause when the FROM clause has a subquery
-- generated by generate_series function
SELECT
	*
FROM
	(SELECT
		*
	FROM
		generate_series(1,10)
	) as gst
WHERE
	generate_series
IN
	(SELECT
		value_2
	FROM
		events_table
	)
ORDER BY
	generate_series ASC;

-- Similar to the test above, now we also have a generate_series in WHERE clause
SELECT
	*
FROM
	(SELECT
		*
	FROM
		generate_series(1,10)
	) as gst
WHERE
	generate_series
IN
	(SELECT
		user_id
	FROM
		users_table
	WHERE
		user_id
	IN
		(SELECT
			*
		FROM
			generate_series(1,3)
		)
	)
ORDER BY
	generate_series ASC;

-- non-colocated subquery in WHERE clause ANDed with false
SELECT count(*)
FROM users_Table
WHERE (FALSE AND EXISTS (SELECT * FROM events_table));

-- multiple non-colocated subqueries in WHERE clause ANDed with false
SELECT count(*)
FROM users_Table
WHERE value_1 IN
    (SELECT value_1
     FROM users_Table) OR (FALSE AND EXISTS (SELECT * FROM events_table));

-- multiple non-colocated subqueries in WHERE clause ANDed with false
SELECT count(*)
FROM users_Table
WHERE value_1 IN
    (SELECT value_1
     FROM users_Table) AND (FALSE AND EXISTS (SELECT * FROM events_table));

-- non-colocated subquery in WHERE clause ANDed with true
SELECT count(*)
FROM users_Table
WHERE (TRUE AND EXISTS (SELECT * FROM events_table));

-- multiple non-colocated subqueries in WHERE clause ANDed with true
SELECT count(*)
FROM users_Table
WHERE value_1 IN
    (SELECT value_1
     FROM users_Table) OR (EXISTS (SELECT * FROM events_table));

-- correlated subquery with aggregate in WHERE
SELECT
    *
FROM
    users_table
WHERE
    user_id IN
    (
        SELECT
            SUM(events_table.user_id)
        FROM
            events_table
        WHERE
            users_table.user_id = events_table.user_id
    )
;

-- correlated subquery with aggregate in HAVING
SELECT
    *
FROM
    users_table
WHERE
    user_id IN
    (
        SELECT
            SUM(events_table.user_id)
        FROM
            events_table
        WHERE
            events_table.user_id = users_table.user_id
        HAVING
            MIN(value_2) > 2
    )
;


-- Local tables also planned recursively, so using it as part of the FROM clause
-- make the clause recurring
CREATE TABLE local_table(id int, value_1 int);
INSERT INTO local_table VALUES(1,1), (2,2);

SELECT
	*
FROM
	(SELECT
		*
	FROM
		local_table) as sub_table
WHERE
	id
IN
	(SELECT
		user_id
	FROM
		users_table);

-- Use local table in WHERE clause
SELECT
	COUNT(*)
FROM
	(SELECT
		*
	FROM
		users_table
	ORDER BY
		user_id
	LIMIT
		10) as sub_table
WHERE
	user_id
IN
	(SELECT
		id
	FROM
		local_table);

-- basic NOT IN correlated subquery
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_2 NOT IN (SELECT value_2 FROM users_table WHERE user_id = e.user_id);

-- correlated subquery with limit
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_2 IN (SELECT value_2 FROM users_table WHERE user_id = e.user_id ORDER BY value_2 LIMIT 1);

-- correlated subquery with distinct
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_2 IN (SELECT DISTINCT (value_3) FROM users_table WHERE user_id = e.user_id);

-- correlated subquery with aggregate
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_2 = (SELECT max(value_2) FROM users_table WHERE user_id = e.user_id);

-- correlated subquery with window function
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_2 IN (SELECT row_number() OVER () FROM users_table WHERE user_id = e.user_id);

-- correlated subquery with group by
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (SELECT min(value_3) FROM users_table WHERE user_id = e.user_id GROUP BY value_2);

SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (SELECT min(value_3) FROM users_table WHERE user_id = e.user_id GROUP BY value_2);


-- correlated subquery with group by
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (SELECT min(value_3) v FROM users_table WHERE user_id = e.user_id GROUP BY e.value_2);

-- correlated subquery with having
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (SELECT min(value_3) v FROM users_table WHERE user_id = e.user_id GROUP BY e.value_2 HAVING min(value_3) > (SELECT 1));

SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (SELECT min(value_3) v FROM users_table WHERE user_id = e.user_id GROUP BY e.value_2 HAVING min(value_3) > (SELECT e.value_3));

-- nested correlated subquery
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (
    SELECT min(r.value_3) v FROM users_reference_table r JOIN (SELECT * FROM users_table WHERE user_id = e.user_id) u USING (user_id)
    WHERE u.value_2 > 3
    GROUP BY e.value_2 HAVING min(r.value_3) > e.value_3);

-- not co-located correlated subquery
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (
    SELECT min(r.value_3) v FROM users_reference_table r JOIN (SELECT * FROM users_table WHERE value_2 = e.user_id) u USING (user_id)
    WHERE u.value_2 > 3
    GROUP BY e.value_2 HAVING min(r.value_3) > e.value_3);

-- cartesian correlated subquery
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (
    SELECT min(r.value_3) v FROM users_reference_table r JOIN users_table u USING (user_id)
    WHERE u.value_2 > 3
    GROUP BY e.value_2 HAVING min(r.value_3) > e.value_3);

-- even more subtle cartesian correlated subquery
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (
    SELECT min(r.value_3) v FROM users_reference_table r JOIN users_table u USING (user_id)
    WHERE u.value_2 > 3
    GROUP BY u.value_2 HAVING min(r.value_3) > e.value_3);

-- not a correlated subquery, uses recursive planning
SELECT
  count(*)
FROM
  events_table e
WHERE
  value_3 IN (
    SELECT min(r.value_3) v FROM users_reference_table r JOIN users_table u USING (user_id)
    WHERE u.value_2 > 3
    GROUP BY r.value_2 HAVING min(r.value_3) > 0);

-- two levels of correlation should also allow
-- merge step in the subquery
SELECT sum(value_1)
FROM users_table u
WHERE EXISTS
    (SELECT 1
     FROM events_table e
     WHERE u.user_id = e.user_id AND
        EXISTS
         (SELECT 1
          FROM users_table u2
          WHERE u2.user_id = u.user_id AND u2.value_1 = 5
          LIMIT 1));

-- correlated subquery in WHERE, with a slightly
-- different syntax that the result of the subquery
-- is compared with a constant
SELECT sum(value_1)
FROM users_table u1
WHERE (SELECT COUNT(DISTINCT e1.value_2)
     FROM events_table e1
     WHERE e1.user_id = u1.user_id
          ) > 115;


-- a correlated subquery which requires merge step
-- can be pushed down on UPDATE/DELETE queries as well
-- rollback to keep the rest of the tests unchanged
BEGIN;
UPDATE users_table u1
 SET value_1 = (SELECT count(DISTINCT value_2)
             	 	 FROM events_table e1
               		WHERE e1.user_id = u1.user_id);

DELETE FROM users_table u1 WHERE (SELECT count(DISTINCT value_2)
             	 	 FROM events_table e1
               		WHERE e1.user_id = u1.user_id) > 10;

ROLLBACK;

-- a correlated anti-join can also be pushed down even if the subquery
-- has a LIMIT
SELECT avg(value_1)
FROM users_table u
WHERE NOT EXISTS
    (SELECT 'XXX'
     FROM events_table e
     WHERE u.user_id = e.user_id and e.value_2 > 10000 LIMIT 1);

-- a [correlated] lateral join can also be pushed down even if the subquery
-- has an aggregate wout a GROUP BY
SELECT
	max(min_of_val_2), max(u1.value_1)
FROM
	users_table u1
		LEFT JOIN LATERAL
	(SELECT min(e1.value_2) as min_of_val_2 FROM events_table e1 WHERE e1.user_id = u1.user_id)  as foo ON (true);


-- a self join is followed by a correlated subquery
EXPLAIN (COSTS OFF)
SELECT
	*
FROM
	users_table u1 JOIN users_table u2 USING (user_id)
WHERE
	u1.value_1 < u2.value_1 AND
	(SELECT
		count(*)
	FROM
		events_table e1
	WHERE
		e1.user_id = u2.user_id) > 10;

-- when the colocated join of the FROM clause
-- entries happen on WHERE clause, Citus cannot
-- pushdown
-- Likely that the colocation checks should be
-- improved
SELECT
	u1.user_id, u2.user_id
FROM
	users_table u1, users_table u2
WHERE
	u1.value_1 < u2.value_1 AND
	(SELECT
		count(*)
	FROM
		events_table e1
	WHERE
		e1.user_id = u2.user_id AND
		u1.user_id = u2.user_id) > 10
ORDER BY 1,2;


-- create a view that contains correlated subquery
CREATE TEMPORARY VIEW correlated_subquery_view AS
	SELECT u1.user_id
	FROM users_table u1
	WHERE (SELECT COUNT(DISTINCT e1.value_2)
	     FROM events_table e1
	     WHERE e1.user_id = u1.user_id
	          ) > 0;

SELECT sum(user_id) FROM correlated_subquery_view;

-- now, join the view with another correlated subquery
SELECT
	sum(mx)
FROM
	correlated_subquery_view
		LEFT JOIN LATERAL
	(SELECT max(value_2) as mx FROM events_table WHERE correlated_subquery_view.user_id = events_table.user_id) as foo ON (true);

-- as an edge case, JOIN is on false
SELECT
	sum(mx)
FROM
	correlated_subquery_view
		LEFT JOIN LATERAL
	(SELECT max(value_2) as mx FROM events_table WHERE correlated_subquery_view.user_id = events_table.user_id) as foo ON (false);


SELECT sum(value_1)
FROM users_table u1
WHERE (SELECT COUNT(DISTINCT e1.value_2)
     FROM events_table e1
     WHERE e1.user_id = u1.user_id AND false
          ) > 115;

SELECT sum(value_1)
FROM users_table u1
WHERE (SELECT COUNT(DISTINCT e1.value_2)
     FROM events_table e1
     WHERE e1.user_id = u1.user_id
          ) > 115 AND false;

SET client_min_messages TO DEFAULT;

DROP TABLE local_table;
DROP SCHEMA subquery_in_where CASCADE;
SET search_path TO public;
