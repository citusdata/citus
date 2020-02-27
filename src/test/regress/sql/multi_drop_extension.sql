--
-- MULTI_DROP_EXTENSION
--
-- Tests around dropping and recreating the extension


SET citus.next_shard_id TO 550000;


CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');

-- this emits a NOTICE message for every table we are dropping with our CASCADE. It would
-- be nice to check that we get those NOTICE messages, but it's nicer to not have to
-- change this test every time the previous tests change the set of tables they leave
-- around.
SET client_min_messages TO 'WARNING';
DROP EXTENSION citus CASCADE;
RESET client_min_messages;

CREATE EXTENSION citus;

-- re-add the nodes to the cluster
SELECT 1 FROM master_add_node(:'worker_1_host', :worker_1_port);
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);

-- verify that a table can be created after the extension has been dropped and recreated
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');
SELECT 1 FROM master_create_empty_shard('testtableddl');
SELECT * FROM testtableddl;
DROP TABLE testtableddl;
