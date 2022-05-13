#include "../udfs/citus_shards_on_worker/11.0-1.sql"
#include "../udfs/citus_shard_indexes_on_worker/11.0-1.sql"
DROP FUNCTION pg_catalog.citus_is_coordinator();
DROP FUNCTION pg_catalog.run_command_on_coordinator(text,boolean);
