SET citus.next_shard_id TO 1600000;

\c "dbname=regression options='-c\ citus.use_secondary_nodes=always'"

CREATE TABLE dest_table (a int, b int);
CREATE TABLE source_table (a int, b int);

-- attempts to change metadata should fail while reading from secondaries
SELECT create_distributed_table('dest_table', 'a');

\c "dbname=regression options='-c\ citus.use_secondary_nodes=never'"

SELECT create_distributed_table('dest_table', 'a');
SELECT create_distributed_table('source_table', 'a');

INSERT INTO dest_table (a, b) VALUES (1, 1);
INSERT INTO dest_table (a, b) VALUES (2, 1);

INSERT INTO source_table (a, b) VALUES (1, 5);
INSERT INTO source_table (a, b) VALUES (10, 10);

-- simulate actually having secondary nodes
SELECT nodeid, groupid, nodename, nodeport, noderack, isactive, noderole, nodecluster FROM pg_dist_node ORDER BY 1, 2;
UPDATE pg_dist_node SET noderole = 'secondary';

\c "dbname=regression options='-c\ citus.use_secondary_nodes=always'"

-- inserts are disallowed
INSERT INTO dest_table (a, b) VALUES (1, 2);

-- router selects are allowed
SELECT a FROM dest_table WHERE a = 1 ORDER BY 1;

-- real-time selects are also allowed
SELECT a FROM dest_table ORDER BY 1;

-- subqueries are also allowed
SET client_min_messages TO DEBUG1;
SELECT
   foo.a
FROM
    (
	     WITH cte AS (
	    SELECT
	    	DISTINCT dest_table.a
	     FROM
	     	dest_table, source_table
	     WHERE
	     	source_table.a = dest_table.a AND
	     dest_table.b IN (1,2,3,4)
	     ) SELECT * FROM cte ORDER BY 1 DESC LIMIT 5
     ) as foo ORDER BY 1;

-- intermediate result pruning should still work
SET citus.log_intermediate_results TO TRUE;

SELECT *
FROM
(
    WITH dest_table_cte AS (
        SELECT a
        FROM dest_table
    ),
    joined_source_table_cte_1 AS (
        SELECT b, a
        FROM source_table
        INNER JOIN dest_table_cte USING (a)
    ),
    joined_source_table_cte_2 AS (
        SELECT b, a
        FROM joined_source_table_cte_1
        INNER JOIN dest_table_cte USING (a)
    )
    SELECT SUM(b) OVER (PARTITION BY coalesce(a, NULL))
    FROM dest_table_cte
    INNER JOIN joined_source_table_cte_2 USING (a)
) inner_query;

-- confirm that the pruning works well when using round-robin as well
SET citus.task_assignment_policy to 'round-robin';
SELECT *
FROM
(
    WITH dest_table_cte AS (
        SELECT a
        FROM dest_table
    ),
    joined_source_table_cte_1 AS (
        SELECT b, a
        FROM source_table
        INNER JOIN dest_table_cte USING (a)
    ),
    joined_source_table_cte_2 AS (
        SELECT b, a
        FROM joined_source_table_cte_1
        INNER JOIN dest_table_cte USING (a)
    )
    SELECT SUM(b) OVER (PARTITION BY coalesce(a, NULL))
    FROM dest_table_cte
    INNER JOIN joined_source_table_cte_2 USING (a)
) inner_query;

SET citus.task_assignment_policy to DEFAULT;
SET client_min_messages TO DEFAULT;
SET citus.log_intermediate_results TO DEFAULT;

-- insert into is definitely not allowed
INSERT INTO dest_table (a, b)
  SELECT a, b FROM source_table;

\c "dbname=regression options='-c\ citus.use_secondary_nodes=never'"
UPDATE pg_dist_node SET noderole = 'primary';
DROP TABLE source_table, dest_table;
