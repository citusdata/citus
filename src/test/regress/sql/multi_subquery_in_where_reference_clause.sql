--
-- queries to test the subquery pushdown on reference tables

-- subqueries in WHERE with IN operator
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 IN  
          (SELECT 
              value_2 
           FROM 
              events_reference_table  
           WHERE 
              users_table.user_id = events_reference_table.user_id
          )
GROUP BY user_id
ORDER BY user_id
LIMIT 3;

-- subqueries in WHERE with NOT EXISTS operator, should work since
-- reference table in the inner part of the join
SELECT 
  user_id
FROM 
  users_table
WHERE 
  NOT EXISTS  
      (SELECT 
          value_2 
       FROM 
          events_reference_table  
       WHERE 
          users_table.user_id = events_reference_table.user_id
      )
GROUP BY user_id
ORDER BY user_id
LIMIT 3;

-- subqueries in WHERE with NOT EXISTS operator, should not work
-- there is a reference table in the outer part of the join
SELECT 
  user_id
FROM 
  users_reference_table
WHERE 
  NOT EXISTS  
      (SELECT 
          value_2 
       FROM 
          events_table  
       WHERE 
          users_reference_table.user_id = events_table.user_id
      )
LIMIT 3;

-- immutable functions are also treated as reference tables
SELECT
  user_id
FROM
  (SELECT user_id FROM generate_series(1,10) AS series(user_id)) users_reference_table
WHERE
  NOT EXISTS
      (SELECT
          value_2
       FROM
          events_table
       WHERE
          users_reference_table.user_id = events_table.user_id
      )
LIMIT 3;

-- subqueries without FROM are also treated as reference tables
SELECT
  user_id
FROM
  (SELECT  5 AS user_id) users_reference_table
WHERE
  NOT EXISTS
      (SELECT
          value_2
       FROM
          events_table
       WHERE
          users_reference_table.user_id = events_table.user_id
      )
LIMIT 3;

-- join with distributed table prevents FROM from recurring
SELECT
  DISTINCT user_id
FROM
  (SELECT s FROM generate_series(1,10) s) series,
  (SELECT DISTINCT user_id FROM users_table) users_table,
  (SELECT 1 AS one) one
WHERE
  s = user_id AND user_id > one AND
  user_id IN
      (SELECT
          value_2
       FROM
          events_table
       WHERE
          users_table.user_id = events_table.user_id
      )
ORDER BY user_id
LIMIT 3;

-- inner join between distributed prevents FROM from recurring
SELECT
  DISTINCT user_id
FROM
  users_table JOIN users_reference_table USING (user_id)
WHERE
  users_table.value_2 IN
      (SELECT
          value_2
       FROM
          events_table
       WHERE
          users_table.user_id = events_table.user_id
      )
ORDER BY user_id
LIMIT 3;

-- outer join could still recur
SELECT
  DISTINCT user_id
FROM
  users_table RIGHT JOIN users_reference_table USING (user_id)
WHERE
  users_table.value_2 IN
      (SELECT
          value_2
       FROM
          events_table
       WHERE
          users_table.user_id = events_table.user_id
      )
ORDER BY user_id
LIMIT 3;

-- subqueries in WHERE with IN operator without equality
SELECT 
  users_table.user_id, count(*)
FROM 
  users_table
WHERE 
  value_2 IN  
          (SELECT 
              value_2 
           FROM 
              events_reference_table  
           WHERE 
              users_table.user_id > events_reference_table.user_id
          )
GROUP BY users_table.user_id
ORDER BY 2 DESC, 1 DESC
LIMIT 3;

-- immutable functions are also treated as reference tables
SELECT
  users_table.user_id, count(*)
FROM
  users_table
WHERE
  value_2 IN
          (SELECT
              value_2
           FROM
              generate_series(1,10) AS events_reference_table(user_id)
           WHERE
              users_table.user_id > events_reference_table.user_id
          )
GROUP BY users_table.user_id
ORDER BY 2 DESC, 1 DESC
LIMIT 3;

-- immutable functions are also treated as reference tables
SELECT
  users_table.user_id, count(*)
FROM
  users_table
WHERE
  value_2 IN
          (SELECT
              value_2
           FROM
              (SELECT 5 AS user_id) AS events_reference_table
           WHERE
              users_table.user_id > events_reference_table.user_id
          )
GROUP BY users_table.user_id
ORDER BY 2 DESC, 1 DESC
LIMIT 3;


-- should error out since reference table exist on the left side 
-- of the left lateral join
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 1 AND value_1 < 3
  AND value_2 >= 5
  AND user_id IN
  (
		SELECT
		  e1.user_id
		FROM (
		  -- Get the first time each user viewed the homepage.
		  SELECT
		    user_id,
		    1 AS view_homepage,
		    min(time) AS view_homepage_time
		  FROM events_reference_table
		     WHERE
		     event_type IN (1, 2)
		  GROUP BY user_id
		) e1 LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS use_demo,
		    time AS use_demo_time
		  FROM events_reference_table
		  WHERE
		    user_id = e1.user_id AND
		       event_type IN (2, 3)
		  ORDER BY time
		) e2 ON true LEFT JOIN LATERAL (
		  SELECT
		    user_id,
		    1 AS enter_credit_card,
		    time AS enter_credit_card_time
		  FROM  events_reference_table
		  WHERE
		    user_id = e2.user_id AND
		    event_type IN (3, 4)
		  ORDER BY time
		) e3 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS submit_card_info,
		    user_id,
		    time AS enter_credit_card_time
		  FROM  events_reference_table
		  WHERE
		    user_id = e3.user_id AND
		    event_type IN (4, 5)
		  ORDER BY time
		) e4 ON true LEFT JOIN LATERAL (
		  SELECT
		    1 AS see_bought_screen
		  FROM  events_reference_table
		  WHERE
		    user_id = e4.user_id AND
		    event_type IN (5, 6)
		  ORDER BY time
		) e5 ON true
		group by e1.user_id
		HAVING sum(submit_card_info) > 0
)
ORDER BY 1, 2;

-- non-partition key equality with reference table
 SELECT 
  user_id, count(*) 
FROM 
  users_table 
WHERE 
  value_3 =ANY(SELECT value_2 FROM users_reference_table WHERE value_1 >= 1 AND value_1 <= 2) 
 GROUP BY 1 ORDER BY 2 DESC, 1 DESC LIMIT 5;


-- non-partition key comparison with reference table
SELECT 
  user_id, count(*)
FROM 
  events_table as e1
WHERE
  event_type IN
            (SELECT 
                event_type
             FROM 
              events_reference_table as e2
             WHERE
              value_2 = 2 AND
              value_3 > 3 AND
              e1.value_2 > e2.value_2
            ) 
GROUP BY 1
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

-- subqueries in both WHERE and FROM clauses
-- should work since reference table is on the 
-- inner part of the join 
SELECT user_id, value_2 FROM users_table WHERE
  value_1 > 1 AND value_1 < 3
  AND value_2 >= 5
  AND user_id IN
  (
    SELECT
      e1.user_id
    FROM (
      -- Get the first time each user viewed the homepage.
      SELECT
        user_id,
        1 AS view_homepage,
        min(time) AS view_homepage_time
      FROM events_table
         WHERE
         event_type IN (1, 2)
      GROUP BY user_id
    ) e1 LEFT JOIN LATERAL (
      SELECT
        user_id,
        1 AS use_demo,
        time AS use_demo_time
      FROM events_table
      WHERE
        user_id = e1.user_id AND
           event_type IN (2, 3)
      ORDER BY time
    ) e2 ON true LEFT JOIN LATERAL (
      SELECT
        user_id,
        1 AS enter_credit_card,
        time AS enter_credit_card_time
      FROM  events_table
      WHERE
        user_id = e2.user_id AND
        event_type IN (3, 4)
      ORDER BY time
    ) e3 ON true LEFT JOIN LATERAL (
      SELECT
        1 AS submit_card_info,
        user_id,
        time AS enter_credit_card_time
      FROM  events_table
      WHERE
        user_id = e3.user_id AND
        event_type IN (4, 5)
      ORDER BY time
    ) e4 ON true LEFT JOIN LATERAL (
      SELECT
        1 AS see_bought_screen
      FROM  events_reference_table
      WHERE
        user_id = e4.user_id AND
        event_type IN (5, 6)
      ORDER BY time
    ) e5 ON true
    group by e1.user_id
    HAVING sum(submit_card_info) > 0
)
ORDER BY 1, 2;

-- reference tables are not allowed if there is sublink
SELECT
  count(*) 
FROM 
  users_reference_table 
WHERE user_id 
  NOT IN
(SELECT users_table.value_2 FROM users_table JOIN users_reference_table as u2 ON users_table.value_2 = u2.value_2);


-- reference tables are not allowed if there is sublink
SELECT count(*)
FROM
  (SELECT 
      user_id, random() FROM users_reference_table) AS vals
    WHERE vals.user_id NOT IN
    (SELECT users_table.value_2
     FROM users_table
     JOIN users_reference_table AS u2 ON users_table.value_2 = u2.value_2);

-- reference tables are not allowed if there is sublink
SELECT user_id,
       count(*)
FROM users_reference_table
WHERE value_2 > ALL
    (SELECT min(value_2)
     FROM events_table
     WHERE event_type > 2 AND users_reference_table.user_id = events_table.user_id
     GROUP BY user_id)
GROUP BY user_id
HAVING count(*) > 3
ORDER BY 2 DESC,
         1 DESC
LIMIT 5;

-- reference tables are not allowed if there is sublink
-- this time in the subquery
SELECT *
FROM users_table
WHERE user_id IN
    (SELECT users_table.user_id
     FROM users_table,
          users_reference_table
     WHERE users_reference_table.user_id NOT IN
         (SELECT value_2
          FROM users_reference_table AS u2))
ORDER BY 1,2,3
LIMIT 5;

-- not supported since GROUP BY references to an upper level query
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 >  
          (SELECT 
              max(value_2) 
           FROM 
              events_reference_table  
           WHERE 
              users_table.user_id = events_reference_table.user_id AND event_type = 2
           GROUP BY
              users_table.user_id
          )
GROUP BY user_id
HAVING count(*) > 3
ORDER BY user_id
LIMIT 5;

-- similar query with slightly more complex group by
-- though the error message is a bit confusing
SELECT 
  user_id
FROM 
  users_table
WHERE 
  value_2 >  
          (SELECT 
              max(value_2) 
           FROM 
              events_reference_table  
           WHERE 
              users_table.user_id = events_reference_table.user_id AND event_type = 2
           GROUP BY
              (users_table.user_id * 2)
          )
GROUP BY user_id
HAVING count(*) > 3
ORDER BY user_id
LIMIT 5;
