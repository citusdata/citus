-- General theme is the following queries
-- SELECT .. FROM distributed_table WHERE [NOT] IN (SELECT FROM subquery_p / distributed_table) 

SET client_min_messages TO DEBUG1;

-- a simple subquery in WHERE clause
SELECT 
	count(*) 
FROM
	users_table
WHERE
	user_id IN (SELECT value_2 FROM events_table);


-- the other way araound
SELECT 
	count(*) 
FROM
	users_table
WHERE
	value_2 IN (SELECT user_id FROM events_table);

-- similar queries with NOT IN
SELECT 
	count(*) 
FROM
	users_table
WHERE
	user_id NOT IN (SELECT value_2 FROM events_table);


-- the other way araound with NOT IN
SELECT 
	count(*) 
FROM
	users_table
WHERE
	value_2 NOT IN (SELECT user_id FROM events_table);


-- similar query with an expression in the target list
SELECT 
	count(*) 
FROM
	users_table
WHERE
	user_id IN (SELECT value_2 / 2 FROM events_table);


-- Subqueries in where clause with greater than operator
SELECT 
	count(*) 
FROM
	users_table
WHERE
	value_1 > (SELECT avg(value_2) FROM events_table WHERE false GROUP BY user_id);


SELECT * FROM users_table WHERE 1 < (SELECT count(*) FROM events_table WHERE user_id > 10 GROUP BY user_id);


-- subquery with ANY operator
SELECT 
	*
FROM 
	users_table 
WHERE 
	value_1= ANY(SELECT user_id FROM events_table) ORDER BY 1,2,3,4 LIMIT 5 ;


SELECT * FROM users_table WHERE row(user_id, value_1) = (SELECT user_id, value_2 FROM events_table WHERE false);

SELECT * FROM users_table WHERE row(user_id, value_1) = (SELECT user_id, value_2 FROM events_table WHERE user_id = 15);


SELECT 
	count(*)
FROM 
	users_table 
WHERE 
 EXISTS (SELECT user_id FROM events_table);

SELECT 
	count(*)
FROM 
	users_table 
WHERE 
 NOT EXISTS (SELECT user_id FROM events_table);

-- this is kind of a weird that, subquery in WHERE would be replaced earlier
-- once we implement that feature
SELECT count(*)
FROM users_table_ref
WHERE user_id NOT IN
    (SELECT users_table.value_2
     FROM users_table
     JOIN users_table_ref AS u2 ON users_table.value_2 = u2.value_2);


SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	value_1 IN (SELECT user_id FROM users_table);

-- replace three queries
SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	users_table.value_1 IN (SELECT user_id FROM users_table) AND
	users_table.value_2 IN (SELECT user_id FROM users_table) AND
	users_table.value_3 IN (SELECT user_id FROM users_table);

-- replace two queries
SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.value_2 IN (SELECT user_id + 1 FROM users_table) AND
	users_table.value_3 IN (SELECT user_id + 2 FROM users_table);

-- replace single query
SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.value_3 IN (SELECT user_id + 2 FROM users_table);

-- replace single query
SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.user_id IN (SELECT user_id + 2 FROM users_table);

-- this is pusbdownable
SELECT 
	count(*) 
FROM 
	users_table, events_table 
WHERE 
	users_table.user_id = events_table.user_id AND 
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.user_id IN (SELECT user_id FROM users_table) AND
	users_table.user_id IN (SELECT user_id FROM users_table);

-- not supported at all
SELECT 
	user_id, AVG(value_1) 
FROM 
	users_table GROUP BY user_id 
HAVING 
	AVG(value_1)>=ALL(SELECT AVG(value_2) FROM events_table GROUP BY user_id);
