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

INSERT INTO source_table (a, b) VALUES (10, 10);

-- simluate actually having secondary nodes
SELECT nodeid, groupid, nodename, nodeport, noderack, isactive, noderole, nodecluster FROM pg_dist_node;
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
SET client_min_messages TO DEFAULT;

-- insert into is definitely not allowed
INSERT INTO dest_table (a, b)
  SELECT a, b FROM source_table;

\c "dbname=regression options='-c\ citus.use_secondary_nodes=never'"
UPDATE pg_dist_node SET noderole = 'primary';
DROP TABLE dest_table;
