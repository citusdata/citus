-- Using the citus schema is a bad idea since many environments use "citus"
-- as the main user and the "citus" schema then sits in front of the
-- search_path.
REVOKE USAGE ON SCHEMA citus FROM public;

-- redefine distributed_tables_colocated to avoid using citus schema
#include "udfs/distributed_tables_colocated/9.0-2.sql"

-- type was used in old version of distributed_tables_colocated
DROP TYPE citus.colocation_placement_type;
