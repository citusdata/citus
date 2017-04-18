--
-- MULTI_DROP_EXTENSION
--
-- Tests around dropping and recreating the extension


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 550000;


CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT master_create_distributed_table('testtableddl', 'distributecol', 'append');

-- this emits a NOTICE message for every table we are dropping with our CASCADE. It would
-- be nice to check that we get those NOTICE messages, but it's nicer to not have to
-- change this test every time the previous tests change the set of tables they leave
-- around.
SET client_min_messages TO 'WARNING';
DROP EXTENSION citus CASCADE;
RESET client_min_messages;

CREATE EXTENSION citus;

-- re-add the nodes to the cluster
SELECT master_add_node('localhost', :worker_1_port);
SELECT master_add_node('localhost', :worker_2_port);

-- verify that a table can be created after the extension has been dropped and recreated
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT master_create_distributed_table('testtableddl', 'distributecol', 'append');
SELECT 1 FROM master_create_empty_shard('testtableddl');
SELECT * FROM testtableddl;
DROP TABLE testtableddl;
