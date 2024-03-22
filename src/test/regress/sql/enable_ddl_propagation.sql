ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

\c - - - :worker_1_port
ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

\c - - - :worker_2_port
ALTER SYSTEM SET citus.enable_ddl_propagation = 'true';
SELECT pg_reload_conf();

