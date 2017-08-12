-- prepare for future tests by configuring all the follower nodes

\c - - - :follower_master_port
ALTER SYSTEM SET citus.use_secondary_nodes TO 'always';
ALTER SYSTEM SET citus.cluster_name TO 'second-cluster';
SELECT pg_reload_conf();

-- also configure the workers, they'll run queries when MX is enabled

\c - - - :follower_worker_1_port
ALTER SYSTEM SET citus.use_secondary_nodes TO 'always';
ALTER SYSTEM SET citus.cluster_name TO 'second-cluster';
SELECT pg_reload_conf();

\c - - - :follower_worker_2_port
ALTER SYSTEM SET citus.use_secondary_nodes TO 'always';
ALTER SYSTEM SET citus.cluster_name TO 'second-cluster';
SELECT pg_reload_conf();
