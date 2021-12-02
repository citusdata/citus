-- citus--10.2-4--11.0-1

-- bump version to 11.0-1
#include "udfs/citus_disable_node/11.0-1.sql"
#include "udfs/create_distributed_function/11.0-1.sql"

#include "udfs/citus_check_connection_to_node/11.0-1.sql"
#include "udfs/citus_internal_add_object_metadata/11.0-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.master_apply_delete_command(text);
DROP FUNCTION pg_catalog.master_get_table_metadata(text);
DROP FUNCTION pg_catalog.master_append_table_to_shard(bigint, text, text, integer);

-- all existing citus local tables are auto converted
-- none of the other tables can have auto-converted as true
ALTER TABLE pg_catalog.pg_dist_partition ADD COLUMN autoconverted boolean DEFAULT false;
ALTER TABLE citus.pg_dist_object ADD COLUMN force_pushdown bool DEFAULT NULL;
UPDATE pg_catalog.pg_dist_partition SET autoconverted = TRUE WHERE partmethod = 'n' AND repmodel = 's';

REVOKE ALL ON FUNCTION start_metadata_sync_to_node(text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION stop_metadata_sync_to_node(text, integer,bool) FROM PUBLIC;

DO LANGUAGE plpgsql
$$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_dist_shard where shardstorage = 'c') THEN
	    RAISE EXCEPTION 'cstore_fdw tables are deprecated as of Citus 11.0'
        USING HINT = 'Install Citus 10.2 and convert your cstore_fdw tables to the columnar access method before upgrading further';
	END IF;
END;
$$;
