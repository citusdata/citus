#include "udfs/worker_create_schema/9.2-2.sql"

-- reserve UINT32_MAX (4294967295) for a special node
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq MAXVALUE 4294967294;
