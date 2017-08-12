\c - - - :master_port

-- do some setup

SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

CREATE TABLE the_table (a int, b int);
SELECT create_distributed_table('the_table', 'a');

INSERT INTO the_table (a, b) VALUES (1, 1);
INSERT INTO the_table (a, b) VALUES (1, 2);

-- connect to the follower and check that a simple select query works, the follower
-- is still in the default cluster and will send queries to the primary nodes

\c - - - :follower_master_port

SELECT * FROM the_table;

-- now, connect to the follower but tell it to use secondary nodes. There are no
-- secondary nodes so this should fail.

-- (this is :follower_master_port but substitution doesn't work here)
\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

SELECT * FROM the_table;

-- add the secondary nodes and try again, the SELECT statement should work this time

\c - - - :master_port

SELECT 1 FROM master_add_node('localhost', :follower_worker_1_port,
  groupid => (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_1_port),
  noderole => 'secondary');
SELECT 1 FROM master_add_node('localhost', :follower_worker_2_port,
  groupid => (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_2_port),
  noderole => 'secondary');

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always'"

-- now that we've added secondaries this should work
SELECT * FROM the_table;

-- okay, now let's play with nodecluster. If we change the cluster of our follower node
-- queries should stat failing again, since there are no worker nodes in the new cluster

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"

-- there are no secondary nodes in this cluster, so this should fail!
SELECT * FROM the_table;

-- now move the secondary nodes into the new cluster and see that the follower, finally
-- correctly configured, can run select queries involving them

\c - - - :master_port
UPDATE pg_dist_node SET nodecluster = 'second-cluster' WHERE noderole = 'secondary';
\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"
SELECT * FROM the_table;

-- clean up after ourselves
\c - - - :master_port
DROP TABLE the_table;
