-- multi subquery pushdown misc aims to test subquery pushdown queries with
--   (i)   Prepared statements
--   (ii)  PL/PGSQL functions
--   (iii) SQL functions

-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
SET citus.enable_router_execution TO false;

PREPARE prepared_subquery_1 AS
SELECT
    user_id,
    user_lastseen,
    array_length(event_array, 1)
FROM (
    SELECT
        user_id,
        max(u.time) as user_lastseen,
        array_agg(event_type ORDER BY u.time) AS event_array
    FROM (
        SELECT user_id, time
        FROM users_table
        WHERE
        user_id >= 1 AND
        user_id <= 3 AND
        users_table.value_1 > 1 AND users_table.value_1 < 3
        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND
          events_table.event_type > 1 AND events_table.event_type < 3
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC, user_id;

EXECUTE prepared_subquery_1;


PREPARE prepared_subquery_2(int, int) AS
SELECT
    user_id,
    user_lastseen,
    array_length(event_array, 1)
FROM (
    SELECT
        user_id,
        max(u.time) as user_lastseen,
        array_agg(event_type ORDER BY u.time) AS event_array
    FROM (
        SELECT user_id, time
        FROM users_table
        WHERE
        user_id >= $1 AND
        user_id <= $2 AND
        users_table.value_1 > 1 AND users_table.value_1 < 3
        ) u LEFT JOIN LATERAL (
          SELECT event_type, time
          FROM events_table
          WHERE user_id = u.user_id AND
          events_table.event_type > 1 AND events_table.event_type < 3
        ) t ON true
        GROUP BY user_id
) AS shard_union
ORDER BY user_lastseen DESC, user_id;

-- should be fine with more than five executions
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);
EXECUTE prepared_subquery_2(1, 3);

-- prepared statements with subqueries in WHERE clause
PREPARE prepared_subquery_3(int, int, int, int, int, int) AS
SELECT user_id
FROM users_table
WHERE user_id IN (SELECT user_id FROM users_table WHERE value_1 >= $4 AND value_1 <= $3)
    AND user_id IN (SELECT user_id FROM users_table WHERE value_1 >= $5 AND value_1 <= $6)
    AND user_id IN (SELECT user_id FROM users_table WHERE  value_1 >= $1 AND value_1 <= $2)
GROUP BY
  user_id
ORDER BY
  user_id DESC
  LIMIT 5;

-- enough times (6+) to actually use prepared statements
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);
EXECUTE prepared_subquery_3(4, 5, 1, 0, 2, 3);


CREATE FUNCTION plpgsql_subquery_test(int, int) RETURNS TABLE(count bigint) AS $$
DECLARE
BEGIN
     RETURN QUERY
			SELECT
			    count(*)
			FROM
			   users_table
			        JOIN
			   (SELECT
			      ma.user_id, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
			    FROM
			    	users_table AS ma, events_table as short_list
			    WHERE
			    	short_list.user_id = ma.user_id and ma.value_1 < $1 and short_list.event_type < 3
			    ) temp
			  ON users_table.user_id = temp.user_id
			  WHERE
			    users_table.value_1 < $2;

END;
$$ LANGUAGE plpgsql;

-- enough times (6+) to actually use prepared statements
SELECT plpgsql_subquery_test(1, 2);
SELECT plpgsql_subquery_test(1, 2);
SELECT plpgsql_subquery_test(1, 2);
SELECT plpgsql_subquery_test(1, 2);
SELECT plpgsql_subquery_test(1, 2);
SELECT plpgsql_subquery_test(1, 2);

-- this should also work, but should return 0 given that int = NULL is always returns false
SELECT plpgsql_subquery_test(1, NULL);

CREATE FUNCTION sql_subquery_test(int, int) RETURNS bigint AS $$
		SELECT
	    count(*)
	FROM
	   users_table
	        JOIN
	   (SELECT
	      ma.user_id, (GREATEST(coalesce(ma.value_4 / 250, 0.0) + GREATEST(1.0))) / 2 AS prob
	    FROM
	    	users_table AS ma, events_table as short_list
	    WHERE
	    	short_list.user_id = ma.user_id and ma.value_1 < $1 and short_list.event_type < 3
	    ) temp
	  ON users_table.user_id = temp.user_id
	  WHERE
	    users_table.value_1 < $2;
$$ LANGUAGE SQL;

-- should error out
SELECT sql_subquery_test(1,1);



-- the joins are actually removed since they are
-- not needed by PostgreSQL planner (e.g., target list
-- doesn't contain anything from there)
-- but Citus can still pushdown this query
SELECT
    t1.user_id, count(*)
FROM users_table t1
LEFT JOIN (
                SELECT
                        user_id
                FROM
                        users_table
                UNION
                        SELECT
                                user_id
                        FROM
                                events_table
) t2 ON t1.user_id = t2.user_id
INNER JOIN (
        SELECT
                user_id
        FROM
                users_table
) t3 ON t1.user_id = t3.user_id
GROUP BY 1
ORDER BY 2 DESC;


-- the joins are actually removed since they are
-- not needed by PostgreSQL planner (e.g., target list
-- doesn't contain anything from there)
-- but Citus can still plan this query even though the query
-- is not safe to pushdown
SELECT
    t1.user_id, count(*)
FROM users_table t1
LEFT JOIN (
                SELECT
                        user_id
                FROM
                        users_table
                UNION
                        SELECT
                                value_2
                        FROM
                                events_table
) t2 ON t1.user_id = t2.user_id
INNER JOIN (
        SELECT
                user_id
        FROM
                users_table
) t3 ON t1.user_id = t3.user_id
GROUP BY 1
ORDER BY 2 DESC;


-- Similar to the above queries, but
-- this time the joins are not removed because
-- target list contains all the entries
SELECT
    *
FROM users_table t1
LEFT JOIN (
                SELECT
                        user_id
                FROM
                        users_table
                UNION
                        SELECT
                                user_id
                        FROM
                                events_table
) t2 ON t1.user_id = t2.user_id
INNER JOIN (
        SELECT
                user_id
        FROM
                users_table
) t3 ON t1.user_id = t3.user_id
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC, 5 DESC, 6 DESC, 7 DESC, 8 DESC
LIMIT 5;

DROP FUNCTION plpgsql_subquery_test(int, int);
DROP FUNCTION sql_subquery_test(int, int);
