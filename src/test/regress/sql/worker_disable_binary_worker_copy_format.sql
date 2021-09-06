-- The files we use in the following text use the text based worker copy
-- format. So we disable the binary worker copy format here.
-- This is a no-op for PG_VERSION_NUM < 14, because the default is off there.
ALTER SYSTEM SET citus.binary_worker_copy_format TO off;
SELECT pg_reload_conf();
SELECT success FROM run_command_on_workers('ALTER SYSTEM SET citus.binary_worker_copy_format TO off');
SELECT success FROM run_command_on_workers('SELECT pg_reload_conf()');

