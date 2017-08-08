ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1600000;

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
SELECT * FROM pg_dist_node;
UPDATE pg_dist_node SET noderole = 'secondary';

\c "dbname=regression options='-c\ citus.use_secondary_nodes=always'"

-- inserts are disallowed
INSERT INTO dest_table (a, b) VALUES (1, 2);

-- router selects are allowed
SELECT a FROM dest_table WHERE a = 1;

-- real-time selects are also allowed
SELECT a FROM dest_table;

-- insert into is definitely not allowed
INSERT INTO dest_table (a, b)
  SELECT a, b FROM source_table;

\c "dbname=regression options='-c\ citus.use_secondary_nodes=never'"
UPDATE pg_dist_node SET noderole = 'primary';
DROP TABLE dest_table;
