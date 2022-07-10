#include "../udfs/citus_shards_on_worker/11.0-1.sql"
#include "../udfs/citus_shard_indexes_on_worker/11.0-1.sql"
#include "../udfs/citus_finalize_upgrade_to_citus11/11.0-1.sql"

DROP FUNCTION  pg_catalog.citus_disable_node(text, integer, bool);
CREATE FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool default false)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_disable_node$$;
COMMENT ON FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool)
    IS 'removes node from the cluster temporarily';

REVOKE ALL ON FUNCTION pg_catalog.citus_disable_node(text,int, bool) FROM PUBLIC;

DROP FUNCTION pg_catalog.citus_is_coordinator();
DROP FUNCTION pg_catalog.run_command_on_coordinator(text,boolean);

DROP FUNCTION pg_catalog.start_metadata_sync_to_all_nodes();
DROP PROCEDURE pg_catalog.citus_finish_citus_upgrade();
