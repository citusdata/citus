-- ===================================================================
-- test recursive planning functionality for non-colocated subqueries
-- We prefered to use EXPLAIN almost all the queries here,
-- otherwise the execution time of so many repartition queries would
-- be too high for the regression tests. Also, note that we're mostly
-- interested in recurive planning side of the things, thus supressing
-- the actual explain output.
-- ===================================================================

SET client_min_messages TO DEBUG1;

CREATE SCHEMA non_colocated_subquery;

SET search_path TO non_colocated_subquery, public;

-- we don't use the data anyway
CREATE TABLE users_table_local AS SELECT * FROM users_table LIMIT 0;
CREATE TABLE events_table_local AS SELECT * FROM events_table LIMIT 0;


SET citus.enable_repartition_joins TO ON;
\set VERBOSITY terse

-- Function that parses explain output as JSON
-- copied from multi_explain.sql and had to give
-- a different name via postfix to prevent concurrent
-- create/drop etc.
CREATE OR REPLACE FUNCTION explain_json_2(query text)
RETURNS jsonb
AS $BODY$
DECLARE
  result jsonb;
BEGIN
  EXECUTE format('EXPLAIN (FORMAT JSON) %s', query) INTO result;
  RETURN result;
END;
$BODY$ LANGUAGE plpgsql;


-- leaf queries contain colocated joins
-- but not the subquery
SELECT true AS valid FROM explain_json_2($$
    SELECT
        foo.value_2
    FROM
        (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.value_2 = bar.value_2;
$$);


-- simple non colocated join with subqueries in WHERE clause
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
        events_table
    WHERE
        event_type
    IN
        (SELECT event_type FROM events_table WHERE user_id < 100);

$$);

-- simple non colocated join with subqueries in WHERE clause with NOT IN
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
        events_table
    WHERE
        user_id
    NOT IN
        (SELECT user_id FROM events_table WHERE event_type = 2);
$$);


-- Subqueries in WHERE and FROM are mixed
-- In this query, only subquery in WHERE is not a colocated join
SELECT true AS valid FROM explain_json_2($$

    SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id, event_type FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id AND
        foo.event_type IN (SELECT event_type FROM events_table WHERE user_id < 3);

$$);


-- Subqueries in WHERE and FROM are mixed
-- In this query, one of the joins in the FROM clause is not colocated
SELECT true AS valid FROM explain_json_2($$

    SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id, event_type FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT (users_table.user_id / 2) as user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id AND
        foo.user_id IN (SELECT user_id FROM events_table WHERE user_id < 10);
$$);

-- Subqueries in WHERE and FROM are mixed
-- In this query, both the joins in the FROM clause is not colocated
SELECT true AS valid FROM explain_json_2($$

    SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id, event_type FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT (users_table.user_id / 2) as user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id AND
        foo.user_id NOT IN (SELECT user_id FROM events_table WHERE user_id < 10);
$$);


-- Subqueries in WHERE and FROM are mixed
-- In this query, one of the joins in the FROM clause is not colocated and subquery in WHERE clause is not colocated
-- similar to the above, but, this time bar is the anchor subquery
SELECT true AS valid FROM explain_json_2($$
    SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id, event_type FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id AND
        foo.event_type IN (SELECT event_type FROM events_table WHERE user_id < 4);
$$);



-- The inner subqueries and the subquery in WHERE are non-located joins
SELECT true AS valid FROM explain_json_2($$
    SELECT foo_top.*, events_table.user_id FROM
    (

            SELECT
            foo.user_id, random()
        FROM
            (SELECT users_table.user_id, event_type FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
            (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.event_type AND event_type IN (5,6,7,8)) as bar
        WHERE
            foo.user_id = bar.user_id AND
            foo.event_type IN (SELECT event_type FROM events_table WHERE user_id = 5)

    ) as foo_top, events_table WHERE events_table.user_id = foo_top.user_id;
$$);

-- Slightly more complex query where there are 5 joins, 1 of them is non-colocated
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
      SELECT
        foo1.user_id, random()
    FROM
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo1,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as foo2,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo3,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (13,14,15,16)) as foo4,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (17,18,19,20)) as foo5
       
    WHERE

        foo1.user_id = foo4.user_id AND
        foo1.user_id = foo2.user_id AND
        foo1.user_id = foo3.user_id AND
        foo1.user_id = foo4.user_id AND
        foo1.user_id = foo5.value_1
    ) as foo_top;

$$);



-- Very similar to the above query
-- One of the queries is not joined on partition key, but this time subquery itself
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
      SELECT
        foo1.user_id, random()
    FROM
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo1,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as foo2,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo3,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (13,14,15,16)) as foo4,
        (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (17,18,19,20)) as foo5
       
        WHERE

        foo1.user_id = foo4.user_id AND
        foo1.user_id = foo2.user_id AND
        foo1.user_id = foo3.user_id AND
        foo1.user_id = foo4.user_id AND
        foo1.user_id = foo5.user_id
    ) as foo_top;
$$);


--  There are two non colocated joins, one is in the one of the leaf queries, 
-- the other is on the top-level subquery
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
      SELECT
        foo1.user_id, random()
    FROM
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo1,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (5,6,7,8)) as foo2,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo3,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (13,14,15,16)) as foo4,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (17,18,19,20)) as foo5   
        WHERE
            foo1.user_id = foo4.user_id AND
            foo1.user_id = foo2.user_id AND
            foo1.user_id = foo3.user_id AND
            foo1.user_id = foo4.user_id AND
            foo1.user_id = foo5.value_1
    ) as foo_top;
$$);


-- a similar query to the above, but, this sime the second
-- non colocated join is on the already recursively planned subquery
-- the results should be the same
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
      SELECT
        foo1.user_id, random()
    FROM
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo1,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (5,6,7,8)) as foo2,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo3,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (13,14,15,16)) as foo4,
            (SELECT users_table.user_id, users_table.value_1 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (17,18,19,20)) as foo5   
        WHERE
            foo1.user_id = foo4.user_id AND
            foo1.user_id = foo2.user_id AND
            foo1.user_id = foo3.user_id AND
            foo1.user_id = foo4.user_id AND
            foo2.user_id = foo5.value_1
    ) as foo_top;
$$);

--  Deeper subqueries are non-colocated
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*) 
    FROM
    (
        SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id) as foo_top JOIN 

    (
        SELECT
        foo.user_id
    FROM
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id) as bar_top 
        ON (foo_top.user_id = bar_top.user_id);
$$);



--  Top level Subquery is not colocated
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*) 
    FROM
    (
        SELECT
        foo.user_id, foo.value_2
    FROM
        (SELECT DISTINCT users_table.user_id, users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id) as foo_top JOIN 

    (
        SELECT
        foo.user_id
    FROM
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo,
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (13,14,15,16)) as bar
    WHERE
        foo.user_id = bar.user_id) as bar_top 
        ON (foo_top.value_2 = bar_top.user_id);  

$$);

--  Top level Subquery is not colocated as the above
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*) 
    FROM
    (
        SELECT
        foo.user_id, foo.value_2
    FROM
        (SELECT DISTINCT users_table.user_id, users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
    WHERE
        foo.user_id = bar.user_id) as foo_top JOIN 
    (
        SELECT
        foo.user_id
    FROM
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as foo,
        (SELECT DISTINCT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (13,14,15,16)) as bar
    WHERE
        foo.user_id = bar.user_id) as bar_top 
    ON (foo_top.value_2 = bar_top.user_id);
$$);



--  non colocated joins are deep inside the query
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*)
    FROM
        (
            SELECT *  FROM 
                (SELECT DISTINCT users_table.user_id FROM users_table, 
                (SELECT events_table.user_id as my_users FROM events_table, users_table WHERE events_table.event_type = users_table.user_id) as foo 
                WHERE foo.my_users = users_table.user_id) as mid_level_query
        ) as bar;
$$);

-- similar to the above, with relation rtes
-- we're able to recursively plan foo
-- note that if we haven't added random() to the subquery, we'd be able run the query
-- via regular repartitioning since PostgreSQL would pull the query up
SELECT true AS valid FROM explain_json_2($$

    SELECT count(*) FROM ( SELECT * FROM 
        (SELECT DISTINCT users_table.user_id FROM users_table, 
        (SELECT events_table.event_type as my_users, random() FROM events_table, users_table WHERE events_table.user_id = users_table.user_id) as foo 
        WHERE foo.my_users = users_table.user_id) as mid_level_query   ) as bar;

$$);


-- same as the above query, but, one level deeper subquery
 SELECT true AS valid FROM explain_json_2($$
 
     SELECT 
         count(*)
     FROM
         (
             SELECT *  FROM 
                 (SELECT DISTINCT users_table.user_id FROM users_table, 
                     (SELECT events_table.user_id as my_users FROM events_table, 
                     (SELECT events_table.user_id, random() FROM users_table, events_table WHERE users_table.user_id = events_table.user_id) as selected_users
                     WHERE events_table.event_type = selected_users.user_id) as foo 
 
                 WHERE foo.my_users = users_table.user_id) as mid_level_query
         ) as bar;
 $$);

-- deeper query, subquery in WHERE clause
-- this time successfull plan the query since the join on the relation and
-- the subquery on the distribution key
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*)
    FROM
        (
            SELECT *  FROM 
                (SELECT DISTINCT users_table.user_id FROM users_table, 
                

                    (SELECT events_table.user_id as my_users FROM events_table, 
                    (SELECT events_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND 

                            users_table.user_id IN (SELECT value_2 FROM events_table)

                        ) as selected_users
                    WHERE events_table.user_id = selected_users.user_id) as foo 

                WHERE foo.my_users = users_table.user_id) as mid_level_query

    ) as bar;

$$);

-- should recursively plan the subquery in WHERE clause
SELECT true AS valid FROM explain_json_2($$SELECT
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

-- leaf subquery repartitioning should work fine when used with CTEs
SELECT true AS valid FROM explain_json_2($$
	WITH q1 AS (SELECT user_id FROM users_table) 
SELECT count(*) FROM q1, (SELECT 
					users_table.user_id, random() 
				FROM 
					users_table, events_table 
				WHERE 
					users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) as bar WHERE bar.user_id = q1.user_id ;$$);

-- subquery joins should work fine when used with CTEs
SELECT true AS valid FROM explain_json_2($$
    WITH q1 AS (SELECT user_id FROM users_table) 
 SELECT count(*) FROM q1, (SELECT 
                    users_table.user_id, random() 
                FROM 
                    users_table, events_table 
                WHERE 
                    users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as bar WHERE bar.user_id = q1.user_id ;$$);


-- should work fine within UNIONs
SELECT true AS valid FROM explain_json_2($$
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.value_2 AND event_type IN (1,2,3,4)) UNION
    (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8));$$);

-- should work fine within leaf queries of deeper subqueries
SELECT true AS valid FROM explain_json_2($$
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



-- this is also supported since we can recursively plan relations as well
-- the relations are joined under a join tree with an alias
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*) 
    FROM
        (users_table u1 JOIN users_table u2 using(value_1)) a JOIN (SELECT value_1, random() FROM users_table) as u3 USING (value_1); 
$$);

-- a very similar query to the above
-- however, this time we users a subquery instead of join alias, and it works
SELECT true AS valid FROM explain_json_2($$

    SELECT 
        count(*) 
    FROM
        (SELECT * FROM users_table u1 JOIN users_table u2 using(value_1)) a JOIN (SELECT value_1, random() FROM users_table) as u3 USING (value_1); 
$$);

-- a similar query to the above, this time subquery is on the left
-- and the relation is on the right of the join tree
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
        (SELECT value_2, random() FROM users_table) as u1
            JOIN
        events_table
            using (value_2);
$$);



-- recursive planning should kick in for outer joins as well
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
        (SELECT value_2, random() FROM users_table) as u1
            LEFT JOIN
        (SELECT value_2, random() FROM users_table) as u2
            USING(value_2);
$$);


-- recursive planning should kick in for outer joins as well
-- but this time recursive planning might convert the query
-- into a not supported join
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
        (SELECT value_2, random() FROM users_table) as u1
            RIGHT JOIN
        (SELECT value_2, random() FROM users_table) as u2
            USING(value_2);
$$);


-- set operations may produce not very efficient plans
-- although we could have picked a as our anchor subquery,
-- we pick foo in this case and recursively plan a
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
        (
         SELECT user_id FROM users_table
            UNION
         SELECT user_id FROM users_table
        ) a 
            JOIN
        (SELECT value_1 FROM users_table) as foo ON (a.user_id = foo.value_1) 
    );
$$);

-- we could do the same with regular tables as well
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    (
        (
         SELECT user_id FROM users_table
            UNION
         SELECT user_id FROM users_table
        ) a 
            JOIN
        users_table as foo ON (a.user_id = foo.value_1) 
    );
$$);

-- this time the the plan is optimial, we are
-- able to keep the UNION query given that foo
-- is the anchor
SELECT true AS valid FROM explain_json_2($$

    SELECT * FROM
    ( 
        (SELECT user_id FROM users_table) as foo 
            JOIN
        (
         SELECT user_id FROM users_table WHERE user_id IN (1,2,3,4)
            UNION
         SELECT user_id FROM users_table WHERE user_id IN (5,6,7,8)
        ) a 

        ON (a.user_id = foo.user_id) 
        JOIN

         (SELECT value_1 FROM users_table) as bar
        
         ON(foo.user_id = bar.value_1) 
    );
$$);

-- it should be safe to recursively plan non colocated subqueries
-- inside a CTE
SELECT true AS valid FROM explain_json_2($$

    WITH non_colocated_subquery AS 
    (
         SELECT
            foo.value_2
        FROM
            (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
            (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (5,6,7,8)) as bar
        WHERE
            foo.value_2 = bar.value_2
    ),
    non_colocated_subquery_2 AS 
    (
        SELECT
            count(*) as cnt
        FROM
            events_table
        WHERE
            event_type
        IN
            (SELECT event_type FROM events_table WHERE user_id < 4)
    )
    SELECT 
        * 
    FROM 
        non_colocated_subquery, non_colocated_subquery_2 
    WHERE 
        non_colocated_subquery.value_2 != non_colocated_subquery_2.cnt
$$);

-- non colocated subquery joins should work fine along with local tables
SELECT true AS valid FROM explain_json_2($$
     SELECT
        count(*)
    FROM
        (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2,3,4)) as foo,
        (SELECT users_table_local.value_2 FROM users_table_local, events_table_local WHERE users_table_local.user_id = events_table_local.user_id AND event_type IN (5,6,7,8)) as bar,
         (SELECT users_table.value_2 FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (9,10,11,12)) as baz
    WHERE
        foo.value_2 = bar.value_2 
        AND 
        foo.value_2 = baz.value_2
$$);

-- a combination of subqueries in FROM and WHERE clauses
SELECT true AS valid FROM explain_json_2($$

    SELECT
        count(*)
    FROM
            (SELECT user_id FROM users_table) as foo 
                JOIN
            (
             SELECT user_id FROM users_table WHERE user_id IN (1,2,3,4)
                UNION
             SELECT user_id FROM users_table WHERE user_id IN (5,6,7,8)
            ) a 

            ON (a.user_id = foo.user_id) 
            JOIN

             (SELECT value_1, value_2 FROM users_table) as bar

             ON(foo.user_id = bar.value_1) 
    WHERE
        value_2 IN (SELECT value_1 FROM users_table WHERE value_2 < 1)
            AND
        value_1 IN (SELECT value_2 FROM users_table WHERE value_1 < 2)
            AND
        foo.user_id IN (SELECT users_table.user_id FROM users_table, events_table WHERE users_table.user_id = events_table.user_id AND event_type IN (1,2))
$$);

-- make sure that we don't pick the refeence table as 
-- the anchor
SELECT true AS valid FROM explain_json_2($$

    SELECT count(*)
    FROM 
      users_reference_table AS users_table_ref,
      (SELECT user_id FROM users_Table) AS foo,
      (SELECT user_id, value_2 FROM events_Table) AS bar
    WHERE 
      users_table_ref.user_id = foo.user_id
      AND foo.user_id = bar.value_2;
$$);

-- make sure to skip calling recursive planning over and over again
-- for already recursively planned subqueries
SET client_min_messages TO DEBUG2;
SELECT *
FROM
  (SELECT *
   FROM users_table
   OFFSET 0) AS users_table
JOIN LATERAL
  (SELECT *
   FROM
     (SELECT *
      FROM events_table
      WHERE user_id = users_table.user_id) AS bar
   LEFT JOIN users_table u2 ON u2.user_id = bar.value_2) AS foo ON TRUE;

-- similar to the above, make sure that we skip recursive plannig when
-- the subquery doesn't have any tables
SELECT true AS valid FROM explain_json_2($$
SELECT *
FROM
  (SELECT 1 AS user_id) AS users_table
JOIN LATERAL
  (SELECT *
   FROM
     (SELECT *
      FROM events_table
      WHERE user_id = users_table.user_id) AS bar
   LEFT JOIN users_table u2 ON u2.user_id = bar.value_2) AS foo ON TRUE
$$);

-- similar to the above, make sure that we skip recursive plannig when
-- the subquery contains only intermediate results
SELECT *
FROM
  (
   SELECT * FROM(
       SELECT *
       FROM users_table
       EXCEPT
       SELECT *
       FROM users_table
       WHERE value_1 > 2
       ) AS users_table_union
   ) AS users_table_limited
JOIN LATERAL
  (SELECT *
   FROM
     (SELECT *
      FROM 
          (SELECT *
       FROM events_table WHERE value_3 > 4
       INTERSECT
       SELECT *
       FROM events_table
       WHERE value_2 > 2
       ) AS events_table
      WHERE user_id = users_table_limited.user_id) AS bar
        LEFT JOIN users_table u2 ON u2.user_id = bar.value_2) AS foo ON TRUE;

-- similar to the above, but this time there are multiple
-- non-colocated subquery joins one of them contains lateral
-- join
SELECT count(*) FROM events_table WHERE user_id NOT IN
(
    SELECT users_table_limited.user_id
    FROM
      (SELECT *
       FROM users_table
       EXCEPT
       SELECT *
       FROM users_table
       WHERE value_1 > 2
       ) AS users_table_limited
    JOIN LATERAL
      (SELECT *
       FROM
         (SELECT *
          FROM 
              (SELECT *
           FROM events_table WHERE value_3 > 4
           INTERSECT
           SELECT *
           FROM events_table
           WHERE value_2 > 2
           ) AS events_table
          WHERE user_id = users_table_limited.user_id) AS bar
            LEFT JOIN users_table u2 ON u2.user_id = bar.value_2) AS foo ON TRUE
  );


-- make sure that non-colocated subquery joins work fine in
-- modifications
CREATE TABLE table1 (id int, tenant_id int);
CREATE VIEW table1_view AS SELECT * from table1 where id < 100;
CREATE TABLE table2 (id int, tenant_id int) partition by range(tenant_id);
CREATE TABLE table2_p1 PARTITION OF table2 FOR VALUES FROM (1) TO (10);

-- modifications on the partitons are only allowed with rep=1
SET citus.shard_replication_factor TO 1;

SELECT create_distributed_table('table2','tenant_id');
SELECT create_distributed_table('table1','tenant_id');

-- all of the above queries are non-colocated subquery joins
-- because the views are replaced with subqueries
UPDATE table2 SET id=20 FROM table1_view WHERE table1_view.id=table2.id;
UPDATE table2_p1 SET id=20 FROM table1_view WHERE table1_view.id=table2_p1.id;

RESET client_min_messages;
DROP FUNCTION explain_json_2(text);

SET search_path TO 'public';
DROP SCHEMA non_colocated_subquery CASCADE;
