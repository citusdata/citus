-- citus--12.1-2--12.1-3

-- bump version to 12.1-3

-- parallel shell-table drop during metadata sync: two new worker-side helpers that
-- let the shell tables be dropped concurrently over the connection pool.
#include "udfs/worker_drop_shell_table_edges/12.1-3.sql"
#include "udfs/worker_drop_shell_table_and_partition_row/12.1-3.sql"
