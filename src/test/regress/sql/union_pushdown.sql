CREATE SCHEMA union_pushdown;
SET search_path TO union_pushdown;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE users_table_part(user_id bigint, value_1 int, value_2 int) PARTITION BY RANGE (value_1);
CREATE TABLE users_table_part_0 PARTITION OF users_table_part FOR VALUES FROM (0) TO (1);
CREATE TABLE users_table_part_1 PARTITION OF users_table_part FOR VALUES FROM (1) TO (2);
CREATE TABLE users_table_part_2 PARTITION OF users_table_part FOR VALUES FROM (2) TO (3);
CREATE TABLE users_table_part_3 PARTITION OF users_table_part FOR VALUES FROM (3) TO (4);
CREATE TABLE users_table_part_4 PARTITION OF users_table_part FOR VALUES FROM (4) TO (5);
CREATE TABLE users_table_part_5 PARTITION OF users_table_part FOR VALUES FROM (5) TO (6);
CREATE TABLE users_table_part_6 PARTITION OF users_table_part FOR VALUES FROM (6) TO (7);
CREATE TABLE users_table_part_7 PARTITION OF users_table_part FOR VALUES FROM (7) TO (8);
CREATE TABLE users_table_part_8 PARTITION OF users_table_part FOR VALUES FROM (8) TO (9);
SELECT create_distributed_table('users_table_part', 'user_id');
INSERT INTO users_table_part SELECT i, i %9, i %50 FROM generate_series(0, 100) i;


CREATE TABLE events_table_part(user_id bigint, value_1 int, value_2 int) PARTITION BY RANGE (value_1);
CREATE TABLE events_table_part_0 PARTITION OF events_table_part FOR VALUES FROM (0) TO (1);
CREATE TABLE events_table_part_1 PARTITION OF events_table_part FOR VALUES FROM (1) TO (2);
CREATE TABLE events_table_part_2 PARTITION OF events_table_part FOR VALUES FROM (2) TO (3);
CREATE TABLE events_table_part_3 PARTITION OF events_table_part FOR VALUES FROM (3) TO (4);
CREATE TABLE events_table_part_4 PARTITION OF events_table_part FOR VALUES FROM (4) TO (5);
CREATE TABLE events_table_part_5 PARTITION OF events_table_part FOR VALUES FROM (5) TO (6);
CREATE TABLE events_table_part_6 PARTITION OF events_table_part FOR VALUES FROM (6) TO (7);
CREATE TABLE events_table_part_7 PARTITION OF events_table_part FOR VALUES FROM (7) TO (8);
CREATE TABLE events_table_part_8 PARTITION OF events_table_part FOR VALUES FROM (8) TO (9);
SELECT create_distributed_table('events_table_part', 'user_id');
INSERT INTO events_table_part SELECT i, i %9, i %50 FROM generate_series(0, 100) i;

CREATE TABLE events_table_ref(user_id bigint, value_1 int, value_2 int);
SELECT create_reference_table('events_table_ref');
INSERT INTO events_table_ref SELECT i, i %9, i %50 FROM generate_series(0, 100) i;

CREATE TABLE events_table_local(user_id bigint, value_1 int, value_2 int);
INSERT INTO events_table_local SELECT i, i %9, i %50 FROM generate_series(0, 100) i;

set client_min_messages to DEBUG1;
-- a union all query with 2 different levels of UNION ALL
SELECT COUNT(*)
FROM
  (SELECT user_id  AS user_id
   FROM
     (SELECT user_id  AS user_id
      FROM users_table_part
      UNION ALL SELECT user_id AS user_id
      FROM users_table_part) AS bar
   UNION ALL SELECT user_id  AS user_id
   FROM users_table_part) AS fool LIMIT 1;

-- a union [all] query with 2 different levels of UNION [ALL]
SELECT COUNT(*)
FROM
  (SELECT user_id  AS user_id
   FROM
     (SELECT user_id  AS user_id
      FROM users_table_part
      UNION ALL SELECT user_id AS user_id
      FROM users_table_part) AS bar
   UNION SELECT user_id  AS user_id
   FROM users_table_part) AS fool LIMIT 1;

-- a union all query with several levels and leaf queries
SELECT DISTINCT user_id
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 3
   UNION ALL
   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part  WHERE value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT DISTINCT user_id AS user_id FROM users_table_part WHERE value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 7) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 8)) AS bar
ORDER BY 1 LIMIT 1;

-- a union all query with several levels and leaf queries
-- on the partition tables
SELECT DISTINCT user_id
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part_2 WHERE value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 3
   UNION ALL
   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part  WHERE value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part_3 WHERE value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT DISTINCT user_id AS user_id FROM users_table_part WHERE value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part_5 WHERE value_1 = 7) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part_4 WHERE value_1 = 8)) AS bar
ORDER BY 1 LIMIT 1;

-- a union all query with a combine query on the coordinator
-- can still be pushed down
SELECT COUNT(DISTINCT user_id)
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 3
   UNION ALL

   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part  WHERE value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 7) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 8)) AS bar;

-- a union all query with ORDER BY LIMIT
SELECT COUNT(user_id)
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 3
   UNION ALL

   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part  WHERE value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 7) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part WHERE value_1 = 8)) AS bar
ORDER BY 1 DESC LIMIT 10;

-- a union all query where leaf queries have JOINs on distribution keys
-- can be pushded down
SELECT COUNT(user_id)
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 3
   UNION ALL

   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 7) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 8  GROUP BY user_id)) AS bar
ORDER BY 1 DESC LIMIT 10;

-- a union all query deep down inside a subquery can still be pushed down
SELECT COUNT(user_id) FROM (
SELECT user_id, random() FROM (
SELECT user_id, random() FROM (
SELECT user_id, random()
FROM
  (SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 1
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 2) AS bar
   UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 3
   UNION ALL
   SELECT user_id AS user_id
   FROM
     (SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 4
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 5) AS bar
   UNION ALL
     (SELECT user_id AS user_id
      FROM
        (SELECT DISTINCT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 6
         UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 7 AND events_table_part.user_id IN (SELECT user_id FROM users_table_part WHERE users_table_part.value_2 = 3 AND events_table_part.user_id IN (SELECT user_id FROM users_table_part WHERE value_2 = 3))) AS bar
      UNION ALL SELECT user_id AS user_id FROM users_table_part JOIN events_table_part USING (user_id) WHERE users_table_part.value_1 = 8  GROUP BY user_id)) AS bar
      WHERE user_id < 2000 ) as level_1 ) as level_2 ) as level_3
ORDER BY 1 DESC LIMIT 10;

-- safe to pushdown
SELECT DISTINCT user_id FROM (
    SELECT * FROM
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as foo
        JOIN
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar
    USING (user_id)
    UNION ALL
    SELECT * FROM
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as foo
        JOIN
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar
    USING (user_id)
) as foo1 ORDER BY 1 LIMIT 1;

-- safe to pushdown
SELECT DISTINCT user_id FROM (
    SELECT * FROM (
    SELECT * FROM
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as foo
        JOIN
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar
    USING (user_id)
    UNION ALL
    SELECT * FROM
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as foo
        JOIN
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar
    USING (user_id)) as bar
) as foo1 ORDER BY 1 LIMIT 1;

-- safe to pushdown
SELECT DISTINCT user_id FROM
	(SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as foo
	JOIN
	(SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar
USING (user_id)
ORDER BY 1 LIMIT 1;

-- safe to pushdown
SELECT * FROM (
    (SELECT user_id FROM users_table_part UNION ALL SELECT * FROM
         (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar1) as foo
    JOIN
    (SELECT user_id FROM users_table_part UNION ALL SELECT * FROM
        (SELECT user_id FROM users_table_part UNION ALL SELECT user_id FROM users_table_part) as bar2) as bar
USING (user_id)
)
ORDER BY 1 LIMIT 1;

-- UNION ALL leaf queries deep in the subquery
SELECT * FROM
(
	SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
	UNION ALL
	SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level ORDER BY 1 DESC LIMIT 3;

-- UNION ALL leaf queries deep in the subquery
-- and random() calls prevent any pullup
SELECT user_id FROM
(
	SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
	UNION ALL
	SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level ORDER BY 1 DESC LIMIT 3;


-- UNION ALL leaf queries deep in the subquery
-- joined with a table
SELECT * FROM
(
	SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users_table_part UNION ALL SELECT * FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
	UNION ALL
	SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users_table_part UNION ALL SELECT * FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level
JOIN
events_table_part USING(user_id)
 ORDER BY 1 DESC LIMIT 3;

-- UNION ALL leaf queries deep in the subquery
-- and random() calls prevent any pullup
SELECT user_id FROM
(
	SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM users_table_part UNION ALL SELECT *,1 FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
	UNION ALL
	SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM users_table_part UNION ALL SELECT *,2 FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level
JOIN
events_table_part USING(user_id)
 ORDER BY 1 DESC LIMIT 3;

-- a tree with   [Q1.1 JOIN Q1.2 UNION ALL Q2.1 JOIN Q2.2] JOIN [Q3.1 JOIN Q3.2 UNION ALL Q4.1 JOIN Q4.2]
-- can be pushed down
SELECT * FROM (
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as left_subquery

	UNION ALL

	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as right_subquery
) as top_level_left
	JOIN
(
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as left_subquery
	UNION ALL
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as right_subquery
) as top_level_righy USING (user_id)
ORDER BY user_id DESC
LIMIT 1;

-- a tree with   [Q1.1 JOIN Q1.2 UNION ALL Q2.1 JOIN Q2.2] JOIN [Q3.1 JOIN Q3.2 UNION ALL Q4.1 JOIN Q4.2]
-- can be pushed down with reference tables
SELECT * FROM (
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l2
      USING(user_id))
  ) as left_subquery

  UNION ALL

  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l2
      USING(user_id))
  ) as right_subquery
) as top_level_left
  JOIN
(
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l2
      USING(user_id))
  ) as left_subquery
  UNION ALL
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_ref USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_ref USING (user_id))) as l2
      USING(user_id))
  ) as right_subquery
) as top_level_righy USING (user_id)
ORDER BY user_id DESC
LIMIT 1;

-- a tree with   [Q1.1 JOIN Q1.2 UNION ALL Q2.1 JOIN Q2.2] JOIN [Q3.1 JOIN Q3.2 UNION ALL Q4.1 JOIN Q4.2]
-- can be pushed down with local tables after local tables have been recursively planned
SELECT * FROM (
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l2
      USING(user_id))
  ) as left_subquery

  UNION ALL

  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l2
      USING(user_id))
  ) as right_subquery
) as top_level_left
  JOIN
(
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l2
      USING(user_id))
  ) as left_subquery
  UNION ALL
  SELECT * FROM
  (
    (((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l1
      JOIN
    ((SELECT * FROM users_table_part JOIN events_table_local USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_local USING (user_id))) as l2
      USING(user_id))
  ) as right_subquery
) as top_level_righy USING (user_id)
ORDER BY user_id DESC
LIMIT 1;

-- a subquery in WHERE clause with
-- a tree with   [Q1.1 JOIN Q1.2 UNION ALL Q2.1 JOIN Q2.2] JOIN [Q3.1 JOIN Q3.2 UNION ALL Q4.1 JOIN Q4.2]
-- can be pushed down with FROM tree consisting of JOINs/UNION ALLs
SELECT * FROM
users_table_part u1
	JOIN
events_table_part e1 USING (user_id)
	JOIN
users_table_part u2 USING (user_id)
	JOIN
(SELECT * FROM users_table_part UNION ALL SELECT * FROM events_table_part) as foo USING (user_id)
WHERE user_id IN
	(SELECT user_id FROM (
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as left_subquery
	UNION ALL
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as right_subquery
) as top_level_left
	JOIN
(
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as left_subquery
	UNION ALL
	SELECT * FROM
	(
		(((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l1
			JOIN
		((SELECT * FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT * FROM users_table_part JOIN events_table_part USING (user_id))) as l2
			USING(user_id))
	) as right_subquery
) as top_level_righy USING (user_id)
ORDER BY user_id DESC
) ORDER BY 1 LIMIT 1;

---------------------------------------------------------------------------
------------ The following tests ensure that we do not accidentally pushdown
------------ queries involving UNION ALL queries if the distribution keys do
------------ not match or any JOIN is not on the distribution key
------------ We used the queries that are defined above
---------------------------------------------------------------------------
RESET client_min_messages;
SELECT public.explain_has_distributed_subplan($$
  EXPLAIN SELECT * FROM ((SELECT 1 FROM events_table_part) UNION ALL (SELECT 1 FROM events_table_part)) u;$$);

SELECT public.explain_has_distributed_subplan($$
  EXPLAIN SELECT * FROM ((SELECT random() FROM events_table_part) UNION ALL (SELECT user_id FROM events_table_part)) u;$$);

SELECT public.explain_has_distributed_subplan($$
  EXPLAIN SELECT * FROM ((SELECT user_id FROM events_table_part) UNION ALL (SELECT user_id - 1 FROM events_table_part)) u;$$);

SELECT public.explain_has_distributed_subplan($$
  EXPLAIN SELECT * FROM ((SELECT user_id FROM events_table_part) UNION ALL (SELECT user_id - 1 as user_id FROM events_table_part)) u
  JOIN users_table_part USING(user_id);$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM
(
  SELECT events_table_part.value_1 FROM users_table_part JOIN events_table_part USING (user_id)
    UNION ALL
  SELECT events_table_part.value_1 FROM users_table_part JOIN events_table_part USING (user_id)
) as bar;$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT COUNT(*)
FROM
  (SELECT user_id  AS user_id
   FROM
     (SELECT value_1  AS user_id
      FROM users_table_part
      UNION ALL SELECT user_id AS user_id
      FROM users_table_part) AS bar
   UNION ALL SELECT user_id  AS user_id
   FROM users_table_part) AS fool$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT COUNT(*)
FROM
  (SELECT user_id  AS user_id
   FROM
     (SELECT count(*)  AS user_id
      FROM users_table_part GROUP BY user_id
      UNION ALL SELECT user_id AS user_id
      FROM users_table_part) AS bar
   UNION ALL SELECT user_id  AS user_id
   FROM users_table_part) AS fool$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM
(
  SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT value_1, user_id FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
  UNION ALL
  SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT user_id, value_1 FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT user_id FROM
(
  SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT value_1 as user_id,random() FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
  UNION ALL
  SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT *,random() FROM (SELECT user_id,random() FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM
  (
    (((SELECT users_table_part.value_1 as user_id FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT users_table_part.user_id as user_id FROM users_table_part JOIN events_table_part USING (user_id))) as l1
      JOIN
    ((SELECT users_table_part.value_1  as user_id FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT users_table_part.user_id as user_id FROM users_table_part JOIN events_table_part USING (user_id))) as l2
      USING(user_id))
  ) as left_subquery
  UNION ALL
  SELECT * FROM
  (
    (((SELECT users_table_part.value_1 as user_id FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT users_table_part.user_id as user_id FROM users_table_part JOIN events_table_part USING (user_id))) as l1
      JOIN
    ((SELECT users_table_part.value_1 as user_id FROM users_table_part JOIN events_table_part USING (user_id)) UNION ALL (SELECT users_table_part.user_id as user_id FROM users_table_part JOIN events_table_part USING (user_id))) as l2
      USING(user_id))
  ) as right_subquery
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM
(
  SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT user_id, value_1 FROM users_table_part UNION ALL SELECT user_id, value_1 FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
  UNION ALL
  SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT value_1, user_id FROM users_table_part UNION ALL SELECT value_1, user_id FROM users_table_part) as level_5) as level_4) as level_3) as level_2) as level_1
) as top_level
JOIN
events_table_part USING(user_id)
 ORDER BY 1 DESC LIMIT 3;
$$);

-- we can pushdown UNION ALL queries that are correlated and exists
-- on the SELECT clause
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
 SELECT
  (SELECT count(*) FROM users_table_part WHERE user_id = e.user_id
      UNION ALL
   SELECT count(*) FROM users_table_part WHERE user_id = e.user_id)
FROM
  (SELECT * FROM users_table_part UNION ALL SELECT * FROM users_table_part) as e;
$$);

-- even if the UNION ALL is not on the distribution key
-- it is safe to pushdown the query because all tables are joined
-- on the distribution keys
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
 SELECT
  (SELECT user_id FROM users_table_part WHERE user_id = e.user_id
      UNION ALL
   SELECT value_1 FROM users_table_part WHERE user_id = e.user_id)
FROM
  (SELECT * FROM users_table_part UNION ALL SELECT * FROM users_table_part) as e;
$$);


-- but if the join is not on the distribution key
-- Citus throws an error
  EXPLAIN
   SELECT
    (SELECT user_id FROM users_table_part WHERE user_id = e.value_1
        UNION ALL
     SELECT user_id FROM users_table_part WHERE user_id = e.value_1)
  FROM
    (SELECT * FROM users_table_part) as e;

-- correlated subquery should be able to pushdown
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT * FROM
users_table_part e JOIN LATERAL
(SELECT value_1 FROM users_table_part WHERE user_id = e.user_id
      UNION ALL
 SELECT value_1 FROM users_table_part WHERE user_id = e.user_id) as foo ON (true);
$$);

-- correlated subquery should be able to pushdown
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT
  (SELECT
    avg(count)
  FROM
    (SELECT count(*) as count from users_table_part where users_table_part.user_id = u_low.user_id
      UNION ALL
     SELECT count(*) from users_table_part where users_table_part.user_id = u_low.user_id) b)
     FROM users_table_part u_low;
$$);


-- we cannot pushdown if one side of the UNION ALL
-- is a reference table
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT *
FROM
  (SELECT *
   FROM events_table_ref
   UNION ALL SELECT events_table_ref.*
   FROM events_table_part
   JOIN events_table_ref USING(user_id)) AS foo
JOIN users_table_part USING(user_id)
LIMIT 1;
$$);

-- we cannot pushdown if one side of the UNION ALL
-- is a local table
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT *
FROM
  (SELECT *
   FROM events_table_local
   UNION ALL SELECT events_table_local.*
   FROM events_table_part
   JOIN events_table_local USING(user_id)) AS foo
JOIN users_table_part USING(user_id)
LIMIT 1;
$$);

-- #4781
CREATE TABLE test_a (a int, b int, id int, k int);
CREATE TABLE test_b (a int, b int, id int, k int);
ALTER TABLE test_a DROP column a;
ALTER TABLE test_b DROP column a;
SELECT create_distributed_table('test_a','id');
SELECT create_distributed_table('test_b','id');

-- try with composite types
CREATE TYPE comp_type AS (
    int_field_1 BIGINT,
    int_field_2 BIGINT
);

CREATE TABLE range_dist_table_2 (dist_col comp_type);
SELECT create_distributed_table('range_dist_table_2', 'dist_col', 'range');

CALL public.create_range_partitioned_shards('range_dist_table_2',
    '{"(10,24)","(10,58)",
	  "(10,90)","(20,100)"}',
	'{"(10,25)","(10,65)",
	  "(10,99)","(20,100)"}');
INSERT INTO range_dist_table_2 VALUES ((10, 24));
INSERT INTO range_dist_table_2 VALUES ((10, 60));
INSERT INTO range_dist_table_2 VALUES ((10, 91));
INSERT INTO range_dist_table_2 VALUES ((20, 100));
-- the following can be pushed down
CREATE OR REPLACE VIEW v2 AS SELECT * from range_dist_table_2 UNION ALL SELECT * from range_dist_table_2;
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(dist_col) FROM v2;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(*) FROM v2;
$$);

DROP TABLE range_dist_table_2 cascade;

-- these should be pushed down.
SELECT public.explain_has_distributed_subplan($$
explain SELECT * FROM users_table_part u1 WHERE (user_id) IN
(
SELECT user_id FROM users_table_part
UNION
SELECT user_id FROM users_table_part
) ;
$$);

SELECT public.explain_has_distributed_subplan($$
explain SELECT * FROM users_table_part u1 WHERE (user_id) IN
(
SELECT user_id FROM (SELECT *, random() FROM users_table_part) as foo
UNION
SELECT user_id FROM (SELECT *, random() FROM users_table_part) as bar
);
$$);

CREATE OR REPLACE VIEW v AS SELECT * from test_a where k>1 UNION SELECT * from test_b where k<1;
-- tests with union
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(*) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT AVG(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT SUM(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT MAX(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(k) FROM v;
$$);


CREATE OR REPLACE VIEW v AS SELECT * from test_a where k>1 UNION ALL SELECT * from test_b where k<1;
-- the followings can be pushed down since dist_key is used in the aggregation
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(id) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT AVG(id) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT SUM(id) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT MAX(id) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT AVG(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT SUM(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT MAX(k) FROM v;
$$);

CREATE OR REPLACE VIEW v AS (SELECT * from (SELECT * from test_a)a1 where k>1) UNION ALL SELECT * from (SELECT * from test_b)b1 where k<1;

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(*) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT AVG(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT SUM(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT MAX(k) FROM v;
$$);

CREATE OR REPLACE VIEW v AS SELECT * from test_a where k<1 UNION ALL SELECT * from test_b where k<1;
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT COUNT(*) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT AVG(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT SUM(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT MAX(k) FROM v;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT count(*) FROM
(
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
UNION
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
) as bar;
$$);

-- we hit the following error hence can't pushdown:
-- Complex subqueries and CTEs are not supported within a UNION
SELECT public.explain_has_distributed_subplan($$
EXPLAIN WITH cte AS (
SELECT * FROM
(
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
UNION
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
) as bar )
SELECT count(*) FROM
(
SELECT bar.user_id FROM cte u1 JOIN LATERAL (SELECT u1.user_id FROM cte u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
UNION
SELECT bar.user_id FROM cte u1 JOIN LATERAL (SELECT u1.user_id FROM cte u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
) as foo;
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT count(*) FROM
(
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id, random() FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
UNION
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u1.user_id, random() FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
) as bar;
$$);

SELECT public.explain_has_distributed_subplan($$
explain
SELECT count(*) FROM
(
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u2.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
UNION
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT u2.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as bar ON (true)
) as bar;
$$);

SELECT public.explain_has_distributed_subplan($$
explain
SELECT bar.user_id FROM users_table_part u1 JOIN LATERAL (SELECT *,random() FROM (SELECT u1.user_id FROM users_table_part u2 WHERE u2.user_id = u1.user_id) as foo) as bar ON (true)
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN WITH cte_1 AS not materialized
(
       SELECT user_id
       FROM   (
                     SELECT *
                     FROM   users_table_part) AS l1
       UNION ALL
       SELECT user_id
       FROM   (
                       SELECT   user_id
                       FROM     users_table_part
                       GROUP BY user_id) AS l2
       UNION ALL
       SELECT user_id
       FROM   (
                     SELECT *
                     FROM   users_table_part) AS l2
       UNION ALL
       SELECT user_id
       FROM   (
                     SELECT *
                     FROM   users_table_part) AS l1
       UNION
       SELECT user_id
       FROM   (
                     SELECT user_id
                     FROM   (
                                            SELECT DISTINCT user_id
                                            FROM            users_table_part) aa ) AS fooo
       UNION
       SELECT user_id
       FROM   (
                              SELECT DISTINCT user_id
                              FROM            users_table_part) AS l2 ), cte_2 AS NOT materialized
(
       SELECT *
       FROM   cte_1), cte_3 AS NOT materialized
(
       SELECT *
       FROM   cte_2), cte_4 AS
(
                SELECT DISTINCT user_id
                FROM            cte_3)
SELECT count(*)
FROM   (
              SELECT user_id
              FROM   (
                            SELECT *
                            FROM   users_table_part) AS l1
              UNION ALL
              SELECT user_id
              FROM   (
                            SELECT *
                            FROM   users_table_part) AS l2
              UNION ALL
              SELECT user_id
              FROM   (
                            SELECT *,
                                   random()
                            FROM   users_table_part) AS l2
              UNION ALL
              SELECT user_id
              FROM   (
                            SELECT *
                            FROM   users_table_part) AS l1
              UNION
              SELECT user_id
              FROM   (
                            SELECT user_id AS user_id
                            FROM   users_table_part) AS l2
              UNION
              SELECT user_id
              FROM   (
                            SELECT *,
                                   random()
                            FROM   users_table_part) AS l2
              UNION
              SELECT user_id
              FROM   (
                            SELECT *
                            FROM   cte_1) AS bar
              UNION ALL
              SELECT user_id
              FROM   cte_2
              UNION
              SELECT *
              FROM   (
                              SELECT   user_id
                              FROM     cte_2
                              GROUP BY user_id) AS bar
              UNION
              SELECT user_id
              FROM   cte_4
              UNION
              SELECT user_id
              FROM   cte_3 ) AS foo
JOIN   lateral
       (
              SELECT *
              FROM   (
                            SELECT user_id
                            FROM   (
                                          SELECT *
                                          FROM   users_table_part) AS l1
                            UNION ALL
                            SELECT user_id
                            FROM   (
                                          SELECT *
                                          FROM   users_table_part) AS l2
                            UNION ALL
                            SELECT user_id
                            FROM   (
                                          SELECT *,
                                                 random()
                                          FROM   users_table_part) AS l2
                            UNION ALL
                            SELECT user_id
                            FROM   (
                                          SELECT *
                                          FROM   users_table_part) AS l1
                            UNION
                            SELECT user_id
                            FROM   (
                                          SELECT user_id AS user_id
                                          FROM   users_table_part) AS l2
                            UNION
                            SELECT user_id
                            FROM   (
                                          SELECT users_table_part.user_id,
                                                 random()
                                          FROM   users_table_part) AS l2
                            UNION
                            SELECT user_id
                            FROM   (
                                          SELECT *
                                          FROM   cte_1) AS bar
                            UNION ALL
                            SELECT user_id
                            FROM   cte_2
                            UNION
                            SELECT *
                            FROM   (
                                            SELECT   user_id
                                            FROM     cte_2
                                            GROUP BY user_id) AS bar
                            UNION
                            SELECT user_id
                            FROM   cte_3 ) AS subqu) AS bar
using  (user_id)
WHERE  (
              foo.user_id) IN
                               (
                               SELECT user_id
                               FROM            (
                                                      SELECT foo.user_id
                                                      FROM   users_table_part u1
                                                      JOIN   lateral
                                                             (
                                                                    SELECT u1.user_id
                                                                    FROM   users_table_part u2
                                                                    WHERE  u2.user_id = u1.user_id) AS foo
                                                      ON     (
                                                                    true)
                                                      UNION
                                                      SELECT foo.user_id
                                                      FROM   users_table_part u1
                                                      JOIN   lateral
                                                             (
                                                                    SELECT u1.user_id
                                                                    FROM   users_table_part u2
                                                                    WHERE  u2.user_id = u1.user_id) AS foo
                                                      ON     (
                                                                    true) ) AS bar
                               UNION
                               SELECT *
                               FROM   cte_2
                               WHERE  user_id IN
                                      (
                                             SELECT user_id
                                             FROM   users_table_part)
                               UNION
                               SELECT *
                               FROM   cte_1 ) ;
$$);


-- we hit https://github.com/citusdata/citus/blob/f00c63c33daf3d16f06462626ca14732b141ae7a/src/backend/distributed/planner/relation_restriction_equivalence.c#L235-L242
SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM users_table_part u1 WHERE (value_1 + random(), user_id) IN
(
SELECT u1.user_id, user_id FROM users_table_part
UNION
SELECT u1.user_id, user_id FROM users_table_part
);
$$);

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT * FROM users_table_part u1 WHERE (user_id) IN
(
SELECT foo.user_id FROM users_table_part JOIN users_table_part foo ON users_table_part.user_id = foo.user_id
UNION
SELECT foo.user_id FROM users_table_part JOIN users_table_part foo on users_table_part.user_id = foo.user_id
);
$$);

CREATE OR REPLACE VIEW v AS SELECT test_a.* from test_a where k>1 UNION ALL SELECT test_b.* from test_b where k<1;

SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT COUNT(*) FROM v;
$$);

CREATE OR REPLACE VIEW v AS (SELECT * from (SELECT * from test_a)a1 where k>1) UNION ALL SELECT * from (SELECT * from test_b)b1 where k<1;
SELECT public.explain_has_distributed_subplan($$
EXPLAIN SELECT COUNT(*) FROM v;
$$);

-- order by prevents postgres from optimizing fields so can be pushed down
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT id, COUNT(*) FROM v GROUP BY id ORDER BY id;
$$);

-- order by is not on dist_key so can't pushed down, needs to be fixed with #4781
SELECT public.explain_has_distributed_subplan($$
EXPLAIN
SELECT k, COUNT(*) FROM v GROUP BY k ORDER BY k;
$$);


SET client_min_messages TO WARNING;
DROP SCHEMA union_pushdown CASCADE;
