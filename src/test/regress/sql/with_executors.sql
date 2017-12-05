-- Confirm we can use local, router, real-time, and task-tracker execution

CREATE SCHEMA with_executors;
SET search_path TO with_executors, public;
SET citus.enable_repartition_joins TO on;

CREATE TABLE with_executors.local_table (id int);
INSERT INTO local_table VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10); 

-- CTEs should be able to use local queries
WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM local_table
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT * FROM local_cte join dist_cte on dist_cte.user_id=local_cte.id
)
SELECT count(*) FROM cte;


WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM local_table
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	),
	merger_cte AS (
		SELECT id as user_id FROM local_cte UNION (SELECT * FROM dist_cte)
	)
	SELECT * FROM merger_cte WHERE user_id IN (1, 2, 3)
)
SELECT * FROM cte ORDER BY 1;


WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM local_table WHERE id < 5
	),
	local_cte_2 AS (
		SELECT * FROM local_table WHERE id > 5
	)
	SELECT local_cte.id as id_1, local_cte_2.id as id_2 FROM local_cte,local_cte_2
)
SELECT 
	* 
FROM 
	cte 
join 
	users_table 
on 
	cte.id_1 = users_table.user_id 
WHERE 
	cte.id_1 IN (3, 4, 5)
ORDER BY
	1,2,3,4,5,6,7
LIMIT
	10;


-- CTEs should be able to use router queries
WITH cte AS (
	WITH router_cte AS (
		SELECT user_id, value_2 FROM users_table WHERE user_id = 1
	),
	router_cte_2 AS (
		SELECT user_id, event_type, value_2 FROM events_table WHERE user_id = 1
	)
	SELECT 
		router_cte.user_id as uid, event_type 
	FROM 
		router_cte, router_cte_2
)
SELECT * FROM cte ORDER BY 2 LIMIT 5;


-- CTEs should be able to use real-time queries
WITH real_time_cte AS (
	SELECT * FROM users_table WHERE value_2 IN (1, 2, 3)
)
SELECT * FROM real_time_cte ORDER BY 1, 2, 3, 4, 5, 6 LIMIT 10;


-- router & real-time together
WITH cte AS (
	WITH router_cte AS (
		SELECT user_id, value_2 FROM users_table WHERE user_id = 1
	),
	real_time AS (
		SELECT user_id, event_type, value_2 FROM events_table
	)
	SELECT 
		router_cte.user_id as uid, event_type 
	FROM 
		router_cte, real_time 
	WHERE 
		router_cte.user_id=real_time.user_id
)
SELECT * FROM cte WHERE uid=1 ORDER BY 2 LIMIT 5;


-- CTEs should be able to use task-tracker queries
WITH cte AS (
	WITH task_tracker_1 AS (
		SELECT 
			users_table.user_id as uid_1, users_table.value_2 
		FROM 
			users_table 
		JOIN
			events_table 
		ON 
			users_table.value_2=events_table.value_2
	),
	task_tracker_2 AS (
		SELECT 
			users_table.user_id as uid_2, users_table.value_3 
		FROM 
			users_table 
		JOIN 
			events_table 
		ON 
			users_table.value_3=events_table.value_3
	)
	SELECT 
		uid_1, uid_2, value_2, value_3
	FROM 
		task_tracker_1
	JOIN
		task_tracker_2
	ON 
		value_2 = value_3
)
SELECT 
	uid_1, uid_2, cte.value_2, cte.value_3 
FROM 
	cte 
JOIN 
	events_table
ON
	cte.value_2 = events_table.event_type
ORDER BY 
	1, 2, 3, 4 
LIMIT 10;


-- All combined
WITH cte AS (
	WITH task_tracker AS (
		SELECT 
			users_table.user_id as uid_1, users_table.value_2 as val_2
		FROM 
			users_table 
		JOIN
			events_table 
		ON 
			users_table.value_2=events_table.value_2
	),
	real_time AS (
		SELECT * FROM users_table
	),
	router_exec AS (
		SELECT * FROM events_table WHERE user_id = 1
	),
	local_table AS (
		SELECT * FROM local_table
	),
	join_first_two AS (
		SELECT uid_1, time, value_3 FROM task_tracker JOIN real_time ON val_2=value_3
	),
	join_last_two AS (
		SELECT 
			router_exec.user_id, local_table.id 
		FROM 
			router_exec 
		JOIN 
			local_table 
		ON 
			router_exec.user_id=local_table.id
	)
	SELECT * FROM join_first_two JOIN join_last_two ON id = value_3 ORDER BY 1,2,3,4,5 LIMIT 10
)
SELECT DISTINCT uid_1, time, value_3 FROM cte ORDER BY 1, 2, 3 LIMIT 20;

-- All combined with outer join
WITH cte AS (
	WITH task_tracker AS (
		SELECT 
			users_table.user_id as uid_1, users_table.value_2 as val_2
		FROM 
			users_table 
		JOIN
			events_table 
		ON 
			users_table.value_2=events_table.value_2
	),
	real_time AS (
		SELECT * FROM users_table
	),
	router_exec AS (
		SELECT * FROM events_table WHERE user_id = 1
	),
	local_table AS (
		SELECT * FROM local_table
	),
	join_first_two AS (
		SELECT uid_1, time, value_3 FROM task_tracker JOIN real_time ON val_2=value_3
	),
	join_last_two AS (
		SELECT 
			router_exec.user_id, local_table.id 
		FROM 
			router_exec 
		JOIN 
			local_table 
		ON 
			router_exec.user_id=local_table.id
	)
	SELECT uid_1, value_3 as val_3 FROM join_first_two JOIN join_last_two ON id = value_3 ORDER BY 1,2 LIMIT 10
)
SELECT DISTINCT uid_1, val_3 FROM cte join events_table on cte.val_3=events_table.event_type ORDER BY 1, 2;


-- CTEs should not be able to terminate (the last SELECT) in a local query
WITH cte AS (
	SELECT * FROM users_table
)
SELECT count(*) FROM cte JOIN local_table ON (user_id = id);

-- CTEs should be able to terminate a router query
WITH cte AS (
	WITH cte_1 AS (
		SELECT * FROM local_table WHERE id < 7
	),
	cte_2 AS (
		SELECT * FROM local_table WHERE id > 3
	),
	cte_dist AS (
		SELECT count(*) as u_id FROM users_table
	),
	cte_merge AS (
		SELECT cte_1.id as id FROM cte_1 join cte_2 on TRUE
	)
	SELECT count(*) FROM users_table join cte_merge on id=user_id
)
SELECT
  row_number() OVER (), count(*)
FROM
  cte, users_table
WHERE
  cte.count=user_id and user_id=5;


-- CTEs should be able to terminate a real-time query
WITH cte AS (
	WITH cte_1 AS (
		SELECT * FROM local_table WHERE id < 7
	),
	cte_2 AS (
		SELECT * FROM local_table WHERE id > 3
	),
	cte_dist AS (
		SELECT count(*) as u_id FROM users_table
	),
	cte_merge AS (
		SELECT cte_1.id as id FROM cte_1 join cte_2 on TRUE
	)
	SELECT count(*) FROM users_table join cte_merge on id=user_id
)
SELECT count(*) FROM cte, users_table where cte.count=user_id;


SET citus.task_executor_type='task-tracker';
-- CTEs shouldn't be able to terminate a task-tracker query
WITH cte_1 AS (
	SELECT 
		u_table.user_id as u_id, e_table.event_type
	FROM
		users_table as u_table
	join
		events_table as e_table
	on
		u_table.value_2=e_table.event_type
	WHERE
		u_table.user_id < 7
),
cte_2 AS (
	SELECT
		u_table.user_id as u_id, e_table.event_type
	FROM
		users_table as u_table
	join
		events_table as e_table
	on
		u_table.value_2=e_table.event_type
	WHERE
		u_table.user_id > 3
),
cte_merge AS (
	SELECT
		cte_1.u_id, cte_2.event_type
	FROM
		cte_1
	join
		cte_2
	on cte_1.event_type=cte_2.u_id
)
SELECT
	count(*)
FROM
	users_table, cte_merge
WHERE
	users_table.user_id = cte_merge.u_id;

DROP SCHEMA with_executors CASCADE;
