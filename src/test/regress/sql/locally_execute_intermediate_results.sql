CREATE SCHEMA locally_execute_intermediate_results;
SET search_path TO locally_execute_intermediate_results;
SET citus.log_intermediate_results TO TRUE;
SET citus.log_local_commands TO TRUE;
SET client_min_messages TO DEBUG1;

SET citus.shard_count TO 4;
SET citus.next_shard_id TO 1580000;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key');

CREATE TABLE ref_table (key int, value text);
SELECT create_reference_table('ref_table');

-- load some data
INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO table_3    VALUES                     (3, '3'), (4, '4'), (5, '5'), (6, '6');
INSERT INTO ref_table  VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');



-- prevent PG 11 - PG 12 outputs to diverge
-- and have a lot more CTEs recursively planned for the 
-- sake of increasing the test coverage
SET citus.enable_cte_inlining TO false;

-- the query cannot be executed locally, but still because of
-- HAVING the intermediate result is written to local file as well
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query can be executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT key FROM cte_2 ORDER BY 1 LIMIT 1)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query should not be executed locally
-- because it can be pushed down with the jointree
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- now all the intermediate results are safe to be in local files
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- multiple CTEs are joined inside HAVING, so written to file
-- locally, but nothing executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1)
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));


-- multiple CTEs are joined inside HAVING, so written to file
-- locally, also the join tree contains only another CTE, so should be
-- executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));

-- now, the CTE is going to be written locally,
-- plus that is going to be read locally because
-- of the aggragate over the cte in HAVING
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max(max) FROM cte_1);


-- two ctes are going to be written locally and executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_1)
SELECT 
	count(*) 
FROM 
	cte_2
GROUP BY key
HAVING max(value) < (SELECT max(max) FROM cte_1);

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING
-- TODO: fixed by #3396
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) > (SELECT value FROM a));

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING -- but used in another subquery/aggregate
-- so one more level of recursive planning 
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) = (SELECT max(value) FROM a));

-- same query as the above, without the aggragate
WITH a AS (SELECT max(key) as key, max(value) as value FROM ref_table)
SELECT count(*),
       key
FROM a JOIN ref_table USING (key)
GROUP BY key
HAVING (max(ref_table.value) <= (SELECT value FROM a));


-- some edge cases around CTEs used inside other CTEs

-- everything can be executed locally
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) FROM cte_2)
SELECT * FROM cte_3;

-- the join between cte_3 and table_2 has to happen remotely
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- the join between cte_3 and table_2 has to happen remotely
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN ref_table USING (key);


\c - - - :worker_1_port

-- now use the same queries on a worker 
SET search_path TO locally_execute_intermediate_results;
SET citus.log_intermediate_results TO TRUE;
SET citus.log_local_commands TO TRUE;
SET client_min_messages TO DEBUG1;

-- prevent PG 11 - PG 12 outputs to diverge
-- and have a lot more CTEs recursively planned for the 
-- sake of increasing the test coverage
SET citus.enable_cte_inlining TO false;

-- the query cannot be executed locally, but still because of
-- HAVING the intermediate result is written to local file as well
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query can be executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT key FROM cte_2 ORDER BY 1 LIMIT 1)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query should not be executed locally
-- because it can be pushed down with the jointree
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- now all the intermediate results are safe to be in local files
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- multiple CTEs are joined inside HAVING, so written to file
-- locally, but nothing executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1)
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));


-- multiple CTEs are joined inside HAVING, so written to file
-- locally, also the join tree contains only another CTE, so should be
-- executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));

-- now, the CTE is going to be written locally,
-- plus that is going to be read locally because
-- of the aggragate over the cte in HAVING
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max(max) FROM cte_1);


-- two ctes are going to be written locally and executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_1)
SELECT 
	count(*) 
FROM 
	cte_2
GROUP BY key
HAVING max(value) < (SELECT max(max) FROM cte_1);

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING
-- TODO: fixed by #3396
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) > (SELECT value FROM a));

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING -- but used in another subquery/aggregate
-- so one more level of recursive planning 
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) = (SELECT max(value) FROM a));

-- same query as the above, without the aggragate
WITH a AS (SELECT max(key) as key, max(value) as value FROM ref_table)
SELECT count(*),
       key
FROM a JOIN ref_table USING (key)
GROUP BY key
HAVING (max(ref_table.value) <= (SELECT value FROM a));


-- some edge cases around CTEs used inside other CTEs

-- everything can be executed locally
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) FROM cte_2)
SELECT * FROM cte_3;

-- the join between cte_3 and table_2 has to can happen
-- locally because the key = 1 resides on this node
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- the join between cte_3 and table_2 has to cannot happen
-- locally because the key = 2 resides on a remote node
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN table_2 USING (key) WHERE table_2.key = 2;

-- the join between cte_3 and ref can happen locally
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN ref_table USING (key);


-- finally, use round-robin policy on the workers with same set of queries
set citus.task_assignment_policy TO "round-robin" ;

-- the query cannot be executed locally, but still because of
-- HAVING the intermediate result is written to local file as well
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query can be executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT key FROM cte_2 ORDER BY 1 LIMIT 1)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- subquery in the WHERE part of the query should not be executed locally
-- because it can be pushed down with the jointree
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2)
SELECT 
	count(*) 
FROM 
	table_2
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- now all the intermediate results are safe to be in local files
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(key) FROM table_2),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
WHERE 
	key > (SELECT max FROM cte_2)
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1);

-- multiple CTEs are joined inside HAVING, so written to file
-- locally, but nothing executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1)
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));


-- multiple CTEs are joined inside HAVING, so written to file
-- locally, also the join tree contains only another CTE, so should be
-- executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT max(value) FROM table_1),
cte_3 AS (SELECT * FROM table_2)
SELECT 
	count(*) 
FROM 
	cte_3
GROUP BY key
HAVING max(value) > (SELECT max FROM cte_1 JOIN cte_2 USING (max));

-- now, the CTE is going to be written locally,
-- plus that is going to be read locally because
-- of the aggragate over the cte in HAVING
WITH cte_1 AS (SELECT max(value) FROM table_1) 
SELECT 
	count(*) 
FROM 
	table_2
GROUP BY key
HAVING max(value) > (SELECT max(max) FROM cte_1);


-- two ctes are going to be written locally and executed locally
WITH cte_1 AS (SELECT max(value) FROM table_1),
cte_2 AS (SELECT * FROM table_1)
SELECT 
	count(*) 
FROM 
	cte_2
GROUP BY key
HAVING max(value) < (SELECT max(max) FROM cte_1);

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING
-- TODO: fixed by #3396
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) > (SELECT value FROM a));

-- this time the same CTE is both joined with a distributed
-- table and used in HAVING -- but used in another subquery/aggregate
-- so one more level of recursive planning 
WITH a AS (SELECT * FROM table_1)
SELECT count(*),
       key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) = (SELECT max(value) FROM a));

-- same query as the above, without the aggragate
WITH a AS (SELECT max(key) as key, max(value) as value FROM ref_table)
SELECT count(*),
       key
FROM a JOIN ref_table USING (key)
GROUP BY key
HAVING (max(ref_table.value) <= (SELECT value FROM a));


-- some edge cases around CTEs used inside other CTEs

-- everything can be executed locally
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) FROM cte_2)
SELECT * FROM cte_3;

-- the join between cte_3 and table_2 has to can happen
-- locally because the key = 1 resides on this node
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN table_2 USING (key) WHERE table_2.key = 1;

-- the join between cte_3 and table_2 has to cannot happen
-- locally because the key = 2 resides on a remote node
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN table_2 USING (key) WHERE table_2.key = 2;

-- the join between cte_3 and ref can happen locally
WITH cte_1 as (SELECT * FROM table_1),
cte_2 AS (SELECT * FROM cte_1),
cte_3 AS (SELECT max(key) as key FROM cte_2)
SELECT * FROM cte_3 JOIN ref_table USING (key);

\c - - - :master_port

SET client_min_messages TO ERROR;
DROP SCHEMA locally_execute_intermediate_results CASCADE;