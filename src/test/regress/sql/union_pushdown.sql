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

RESET client_min_messages;
DROP SCHEMA union_pushdown CASCADE;
