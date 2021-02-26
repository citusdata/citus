-- this test file relies on multi_behavioral_analytics_create_table
-- and aims to have variety of tests covering CROSS JOINs
-- "t1 CROSS JOIN t2" is equivalent of "t1 JOIN t2 ON true"

-- a distributed table can be cross joined with a reference table
-- and the CROSS JOIN can be in the outer part of an outer JOIN
SELECT count(*) FROM events_reference_table e1 CROSS JOIN events_table e2 LEFT JOIN users_table u ON (e2.user_id = u.user_id);

-- two distributed tables cannot be cross joined
-- as it lacks distribution key equality
SELECT count(*) FROM events_reference_table e1 CROSS JOIN events_table e2 CROSS JOIN users_table u;
SELECT count(*) FROM events_reference_table e1, events_table e2, users_table u;

-- we can provide the distribution key equality via WHERE clause
SELECT count(*) FROM events_reference_table e1 CROSS JOIN events_table e2 CROSS JOIN users_table u WHERE u.user_id = e2.user_id;


-- two reference tables are JOINed, and later CROSS JOINed with a distributed table
-- it is safe to pushdown
SELECT count(*) FROM users_ref_test_table ref1 INNER JOIN users_ref_test_table ref2 on ref1.id = ref2.id CROSS JOIN users_table;
SELECT count(*) FROM users_ref_test_table ref1 LEFT JOIN users_ref_test_table ref2 on ref1.id = ref2.id CROSS JOIN users_table;
SELECT count(*) FROM users_ref_test_table ref1 RIGHT JOIN users_ref_test_table ref2 on ref1.id = ref2.id CROSS JOIN users_table;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 CROSS JOIN users_table;

-- two reference tables CROSS JOINNed, and later JOINED with distributed tables
-- it is safe to pushdown
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 RIGHT JOIN users_table ON false;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2  JOIN users_table ON false;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2  JOIN users_table ON true;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 RIGHT JOIN users_table ON true;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 RIGHT JOIN users_table ON (ref1.id = users_table.user_id);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 JOIN users_table ON (ref1.id = users_table.user_id);

-- two reference tables CROSS JOINNed, and later JOINED with distributed tables
-- but the reference table CROSS JOIN is in the outer side of the JOIN with the distributed table
-- so we cannot pushdown
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (ref1.id = users_table.user_id);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 FULL JOIN users_table ON (ref1.id = users_table.user_id);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (ref1.id != users_table.user_id);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (ref1.id > 0);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (users_table.user_id > 0);
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON true;
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON false;

-- a reference tables CROSS JOINed with a distribted table, and later JOINED with distributed tables on distribution keys
-- so safe to pushdown
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 JOIN users_table u2 ON (u1.user_id = u2.user_id);
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table u2 ON (u1.user_id = u2.user_id);
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 FULL JOIN users_table u2 ON (u1.user_id = u2.user_id);
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 RIGHT JOIN users_table u2 ON (u1.user_id = u2.user_id);

-- a reference tables CROSS JOINed with a distribted table, and later JOINED with distributed tables on reference table column
-- so not safe to pushdown
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table u2 ON (ref2.id = u2.user_id);
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 FULL JOIN users_table u2 ON (ref2.id = u2.user_id);
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 RIGHT JOIN users_table u2 ON (ref2.id = u2.user_id);

-- via repartitioning, Citus can handle this query as the result of "u1 CROSS JOIN ref2"
-- can be repartitioned on ref2.id
Set citus.enable_repartition_joins to on;
SELECT count(*) FROM users_table u1 CROSS JOIN users_ref_test_table ref2 JOIN users_table u2 ON (ref2.id = u2.user_id);
reset citus.enable_repartition_joins;

-- although the following has the "ref LEFT JOIN dist" type of query, the LEFT JOIN is eliminated by Postgres
-- because the INNER JOIN eliminates the LEFT JOIN
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (ref1.id = users_table.user_id) JOIN users_table u2 ON (u2.user_id = users_table.user_id);

-- this is the same query as the above, but this time the outer query is also LEFT JOIN, meaning that Postgres
-- cannot eliminate the outer join
SELECT count(*) FROM users_ref_test_table ref1 CROSS JOIN users_ref_test_table ref2 LEFT JOIN users_table ON (ref1.id = users_table.user_id) LEFT JOIN users_table u2 ON (u2.user_id = users_table.user_id);

-- cross join that goes through non-colocated subquery logic
-- for the "events_table" subquery as both distributed tables
-- do not have JOIN on the distribution key
SELECT max(events_all.cnt),
      events_all.usr_id
	FROM
(SELECT *, random() FROM
	(SELECT *, random()
			FROM (SELECT users_table.user_id AS usr_id, count(*) AS cnt
					FROM (SELECT *,random FROM (SELECT *, random() FROM events_reference_table) as events_reference_table) as events_reference_table
					INNER JOIN users_table ON (users_table.user_id = events_reference_table.user_id)
					GROUP BY users_table.user_id) AS events_all_inner
          ) AS events_all
	) AS events_all
	CROSS JOIN (SELECT *,random() FROM (SELECT *, random() FROM  events_table)  as events_table) as events_table
GROUP BY 2
ORDER BY 1 DESC,
         2 DESC
LIMIT 5;

-- cross join that goes through non-colocated subquery logic
-- for the "events_all" subquery as both distributed tables
-- do not have JOIN on the distribution key
SELECT max(events_all.cnt),
       events_all.usr_id
	FROM events_table
	CROSS JOIN (SELECT *, random()
			FROM (SELECT users_table.user_id AS usr_id, count(*) AS cnt
					FROM events_reference_table
					INNER JOIN users_table ON (users_table.user_id = events_reference_table.user_id)
					GROUP BY users_table.user_id) AS events_all_inner
          ) AS events_all
GROUP BY 2
ORDER BY 1 DESC,
         2 DESC
LIMIT 5;


-- cross join is between a reference table and distributed table, and
-- deep inside a subquery. The subquery can be in the outer part of the LEFT JOIN
SELECT
	users_table.*
FROM
	(SELECT
		events_all.*, random()
	FROM
			events_reference_table JOIN users_table USING(user_id)
		JOIN
			(SELECT *, random()
				FROM (SELECT users_table.user_id AS usr_id, count(*) AS cnt
						FROM (SELECT *,random FROM (SELECT *, random() FROM events_reference_table) as events_reference_table) as events_reference_table
						CROSS JOIN users_table
						GROUP BY users_table.user_id) AS events_all_inner
	          ) AS events_all ON (user_id = usr_id)
	) AS events_all
	LEFT JOIN (SELECT *,random() FROM (SELECT *, random() FROM  events_table)  as events_table) as events_table ON (events_all.usr_id = events_table.user_id)
	LEFT JOIN users_table USING (user_id)
ORDER BY 1,2,3,4 LIMIT 5;

-- we don't support cross JOINs between distributed tables
-- and without target list entries
CREATE TABLE dist1(c0 int);
CREATE TABLE dist2(c0 int);
CREATE TABLE dist3(c0 int , c1 int);
CREATE TABLE dist4(c0 int , c1 int);

SELECT create_distributed_table('dist1', 'c0');
SELECT create_distributed_table('dist2', 'c0');
SELECT create_distributed_table('dist3', 'c1');
SELECT create_distributed_table('dist4', 'c1');

SELECT dist2.c0 FROM dist1, dist3, dist4, dist2 WHERE (dist3.c0) IN (dist4.c0);
SELECT 1 FROM dist3, dist4, dist2 WHERE (dist3.c0) IN (dist4.c0);
SELECT  FROM dist3, dist4, dist2 WHERE (dist3.c0) IN (dist4.c0);
SELECT dist2.c0 FROM dist3, dist4, dist2 WHERE (dist3.c0) IN (dist4.c0);
SELECT dist2.* FROM dist3, dist4, dist2 WHERE (dist3.c0) IN (dist4.c0);
