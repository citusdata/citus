--
-- MX_COORDINATOR_SHOULDHAVESHARDS
--
-- Test queries on a distributed table with shards on the coordinator
--
-- This test file has an alternative output because of the change in the
-- display of SQL-standard function's arguments in INSERT/SELECT in PG15.
-- The alternative output can be deleted when we drop support for PG14
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS server_version_above_fourteen;

CREATE SCHEMA mx_coordinator_shouldhaveshards;
SET search_path TO mx_coordinator_shouldhaveshards;

SET citus.shard_replication_factor to 1;
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

-- issue 4508 table_1 and table_2 are used to test some edge cases
-- around intermediate result pruning
CREATE TABLE table_1 (key int, value text);
SELECT create_distributed_table('table_1', 'key', colocate_with := 'none');

CREATE TABLE table_2 (key int, value text);
SELECT create_distributed_table('table_2', 'key', colocate_with := 'none');

INSERT INTO table_1    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');

SET citus.shard_replication_factor to 2;

CREATE TABLE table_1_rep (key int, value text);
SELECT create_distributed_table('table_1_rep', 'key', colocate_with := 'none');

CREATE TABLE table_2_rep (key int, value text);
SELECT create_distributed_table('table_2_rep', 'key', colocate_with := 'none');

INSERT INTO table_1_rep    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4');
INSERT INTO table_2_rep    VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5'), (6, '6');


set citus.log_intermediate_results TO ON;
set client_min_messages to debug1;

WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
INSERT INTO table_1 SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH stats AS (
  SELECT count(key) m FROM table_1
),
inserts AS (
  INSERT INTO table_2
  SELECT key, count(*)
  FROM table_1
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) <= (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;

WITH a AS (SELECT * FROM table_1_rep ORDER BY 1,2 DESC LIMIT 1)
SELECT count(*),
key
FROM a JOIN table_2_rep USING (key)
GROUP BY key
HAVING (max(table_2_rep.value) >= (SELECT value FROM a));

WITH a AS (SELECT * FROM table_1_rep ORDER BY 1,2 DESC LIMIT 1)
INSERT INTO table_1_rep SELECT count(*),
key
FROM a JOIN table_2_rep USING (key)
GROUP BY key
HAVING (max(table_2_rep.value) >= (SELECT value FROM a));

WITH stats AS (
  SELECT count(key) m FROM table_1_rep
),
inserts AS (
  INSERT INTO table_2_rep
  SELECT key, count(*)
  FROM table_1_rep
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) <= (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;

\c - - - :worker_1_port
SET search_path TO mx_coordinator_shouldhaveshards;

set citus.log_intermediate_results TO ON;
set client_min_messages to debug1;

WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH a AS (SELECT * FROM table_1 ORDER BY 1,2 DESC LIMIT 1)
INSERT INTO table_1 SELECT count(*),
key
FROM a JOIN table_2 USING (key)
GROUP BY key
HAVING (max(table_2.value) >= (SELECT value FROM a));

WITH stats AS (
  SELECT count(key) m FROM table_1
),
inserts AS (
  INSERT INTO table_2
  SELECT key, count(*)
  FROM table_1
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) <= (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;

WITH a AS (SELECT * FROM table_1_rep ORDER BY 1,2 DESC LIMIT 1)
SELECT count(*),
key
FROM a JOIN table_2_rep USING (key)
GROUP BY key
HAVING (max(table_2_rep.value) >= (SELECT value FROM a));

WITH a AS (SELECT * FROM table_1_rep ORDER BY 1,2 DESC LIMIT 1)
INSERT INTO table_1_rep SELECT count(*),
key
FROM a JOIN table_2_rep USING (key)
GROUP BY key
HAVING (max(table_2_rep.value) >= (SELECT value FROM a));

WITH stats AS (
  SELECT count(key) m FROM table_1_rep
),
inserts AS (
  INSERT INTO table_2_rep
  SELECT key, count(*)
  FROM table_1_rep
  WHERE key >= (SELECT m FROM stats)
  GROUP BY key
  HAVING count(*) <= (SELECT m FROM stats)
  LIMIT 1
  RETURNING *
) SELECT count(*) FROM inserts;

\c - - - :master_port

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', false);
SET client_min_messages TO ERROR;
DROP SCHEMA mx_coordinator_shouldhaveshards CASCADE;

SELECT master_remove_node('localhost', :master_port);
