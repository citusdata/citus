-- ===================================================================
-- test recursive planning functionality on partitioned tables
-- ===================================================================
CREATE SCHEMA subquery_and_partitioning;
SET search_path TO subquery_and_partitioning, public;


CREATE TABLE users_table_local AS SELECT * FROM users_table;
CREATE TABLE events_table_local AS SELECT * FROM events_table;

CREATE TABLE partitioning_test(id int, value_1 int, time date) PARTITION BY RANGE (time);
 
-- create its partitions
CREATE TABLE partitioning_test_2017 PARTITION OF partitioning_test FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, 1, '2017-11-23');
INSERT INTO partitioning_test VALUES (2, 1, '2010-07-07');

INSERT INTO partitioning_test_2017 VALUES (3, 3, '2017-11-22');
INSERT INTO partitioning_test_2010 VALUES (4, 4, '2010-03-03');

-- distribute partitioned table
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('partitioning_test', 'id');

SET client_min_messages TO DEBUG1;

-- subplan for partitioned tables
SELECT
   id
FROM
    (SELECT 
    	DISTINCT partitioning_test.id 
     FROM 
     	partitioning_test
     LIMIT 5
     ) as foo
    ORDER BY 1 DESC;

-- final query is router on partitioned tables
SELECT
   *
FROM
    (SELECT 
    	DISTINCT partitioning_test.id 
     FROM 
     	partitioning_test
     LIMIT 5
     ) as foo,
	(SELECT 
    	DISTINCT partitioning_test.time 
     FROM 
     	partitioning_test
     LIMIT 5
     ) as bar
	WHERE foo.id = date_part('day', bar.time)
	ORDER BY 2 DESC, 1;

-- final query is real-time
SELECT
   *
FROM
    (SELECT 
    	DISTINCT partitioning_test.time 
     FROM 
     	partitioning_test
 	 ORDER BY 1 DESC
     LIMIT 5
     ) as foo,
	(
		SELECT 
	    	DISTINCT partitioning_test.id 
	     FROM 
	     	partitioning_test
     ) as bar
	WHERE  date_part('day', foo.time) = bar.id
	ORDER BY 2 DESC, 1 DESC
	LIMIT 3;

-- final query is real-time that is joined with partitioned table
SELECT
   *
FROM
    (SELECT 
    	DISTINCT partitioning_test.time 
     FROM 
     	partitioning_test
 	 ORDER BY 1 DESC
     LIMIT 5
     ) as foo,
	(
		SELECT 
	    	DISTINCT partitioning_test.id 
	     FROM 
	     	partitioning_test
     ) as bar, 
	partitioning_test
	WHERE  date_part('day', foo.time) = bar.id AND partitioning_test.id = bar.id
	ORDER BY 2 DESC, 1 DESC
	LIMIT 3;

-- subquery in WHERE clause
SELECT DISTINCT id
FROM partitioning_test
WHERE 
	id IN (SELECT DISTINCT date_part('day', time) FROM partitioning_test);

-- repartition subquery
SET citus.enable_repartition_joins to ON;
SELECT 
	count(*) 
FROM
(
	SELECT DISTINCT p1.value_1 FROM partitioning_test as p1, partitioning_test as p2 WHERE p1.id = p2.value_1
) as foo, 
(
	SELECT user_id FROM users_table
) as bar
WHERE foo.value_1 = bar.user_id; 
SET citus.enable_repartition_joins to OFF;


-- subquery, cte, view and non-partitioned tables
CREATE VIEW subquery_and_ctes AS 
SELECT 
	* 
FROM 
(

	WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT 
			user_id
		FROM 
			events_table, 
			(SELECT DISTINCT value_1 FROM partitioning_test OFFSET 0) as foo
		WHERE 
			events_table.user_id = foo.value_1 AND
			events_table.user_id IN (SELECT DISTINCT value_1 FROM users_table ORDER BY 1 LIMIT 3)
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*)  as cnt
FROM 
	cte,
	  (SELECT 
    	DISTINCT events_table.user_id 
     FROM 
     	partitioning_test, events_table
     WHERE 
     	events_table.user_id = partitioning_test.id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id

) as foo, users_table WHERE foo.cnt > users_table.value_2;

SELECT * FROM subquery_and_ctes
ORDER BY 3 DESC, 1 DESC, 2 DESC, 4 DESC
LIMIT 5;

-- deep subquery, partitioned and non-partitioned tables together
SELECT count(*)
FROM
(
	SELECT avg(min) FROM 
	(
		SELECT min(partitioning_test.value_1) FROM
		(
			SELECT avg(event_type) as avg_ev_type FROM 
			(
				SELECT 
					max(value_1) as mx_val_1 
					FROM (
							SELECT 
								avg(event_type) as avg
							FROM
							(
								SELECT 
									cnt 
								FROM 
									(SELECT count(*) as cnt, value_1 FROM partitioning_test GROUP BY value_1) as level_1, users_table
								WHERE 
									users_table.user_id = level_1.cnt
							) as level_2, events_table
							WHERE events_table.user_id = level_2.cnt 
							GROUP BY level_2.cnt
						) as level_3, users_table
					WHERE user_id = level_3.avg
					GROUP BY level_3.avg
					) as level_4, events_table
				WHERE level_4.mx_val_1 = events_table.user_id
				GROUP BY level_4.mx_val_1
				) as level_5, partitioning_test
				WHERE 
					level_5.avg_ev_type = partitioning_test.id
				GROUP BY 
					level_5.avg_ev_type
		) as level_6, users_table WHERE users_table.user_id = level_6.min
	GROUP BY users_table.value_1
	) as bar;

SET client_min_messages TO DEFAULT;

DROP SCHEMA subquery_and_partitioning CASCADE;
SET search_path TO public;