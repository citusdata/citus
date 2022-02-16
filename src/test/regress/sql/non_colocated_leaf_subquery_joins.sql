-- ===================================================================
-- test recursive planning functionality for non-colocated subqueries
-- We preferred to use EXPLAIN almost all the queries here,
-- otherwise the execution time of so many repartition queries would
-- be too high for the regression tests. Also, note that we're mostly
-- interested in recurive planning side of the things, thus supressing
-- the actual explain output.
-- ===================================================================

SET client_min_messages TO DEBUG1;
SET log_error_verbosity TO TERSE;

\set VERBOSITY terse
SET citus.enable_repartition_joins TO ON;

-- Function that parses explain output as JSON
-- copied from multi_explain.sql
CREATE OR REPLACE FUNCTION explain_json(query text)
RETURNS jsonb
AS $BODY$
DECLARE
  result jsonb;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT JSON) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;

SHOW log_error_verbosity;
-- should recursively plan foo
SELECT true AS valid FROM explain_json($$SELECT
    count(*)
FROM
    (SELECT users_table.user_id, random() FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
WHERE
    foo.user_id = bar.user_id;$$);

 -- should recursively plan both foo and bar
SELECT true AS valid FROM explain_json($$SELECT
    count(*)
FROM
    (SELECT users_table.user_id, random() FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (5,6,7,8)) as bar
WHERE
    foo.user_id = bar.user_id;$$);


-- should recursively plan the subquery in WHERE clause
SELECT true AS valid FROM explain_json($$SELECT
	count(*)
FROM
	users_table
WHERE
	value_1
		IN
	(SELECT
		users_table.user_id
	 FROM
	 	users_table, events_table
	 WHERE
	 	users_table.user_id = events_table.value_2 AND event_type IN (5,6));$$);

-- should work fine when used with CTEs
SELECT true AS valid FROM explain_json($$
	WITH q1 AS (SELECT user_id FROM users_table)
SELECT count(*) FROM q1, (SELECT
					users_table.user_id, random()
				FROM
					users_table, events_table
				WHERE
					users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as bar WHERE bar.user_id = q1.user_id ;$$);

-- should work fine within UNIONs
SELECT true AS valid FROM explain_json($$
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) UNION
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8));$$);

-- should work fine within leaf queries of deeper subqueries
SELECT true AS valid FROM explain_json($$
SELECT event, array_length(events_table, 1)
FROM (
  SELECT event, array_agg(t.user_id) AS events_table
  FROM (
    SELECT
    	DISTINCT ON(e.event_type::text) e.event_type::text as event, e.time, e.user_id
    FROM
    	users_table AS u,
        events_table AS e,
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (5,6,7,8)) as bar
    WHERE u.user_id = e.user_id AND
    		u.user_id IN
    		(
    			SELECT
    				user_id
    			FROM
    				users_table
    			WHERE value_2 >= 5
			    AND  EXISTS (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4))
				LIMIT 5
    		)
  ) t, users_table WHERE users_table.value_1 = t.event::int
  GROUP BY event
) q
ORDER BY 2 DESC, 1;
$$);

 -- should recursively plan bar subquery given that it is not joined
 -- on the distribution key with bar
SELECT true AS valid FROM explain_json($$SELECT
    count(*)
FROM
    (SELECT users_table.user_id, random() FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
    (SELECT users_table.user_id, value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
WHERE
    foo.user_id = bar.value_1;$$);

SET log_error_verbosity TO DEFAULT;
SET client_min_messages TO DEFAULT;
SET citus.enable_repartition_joins TO DEFAULT;

DROP FUNCTION explain_json(text);
