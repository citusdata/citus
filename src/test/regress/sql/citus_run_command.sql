SELECT citus_run_local_command($$SELECT 1; SELECT 1$$);
SELECT citus_run_local_command($$SELECT 1; SELECT 1/0$$);
SELECT citus_run_local_command(NULL);

