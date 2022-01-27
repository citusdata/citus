-- citus--11.0-1--10.2-4

DROP FUNCTION pg_catalog.create_distributed_function(regprocedure, text, text, bool);
CREATE FUNCTION pg_catalog.master_apply_delete_command(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_apply_delete_command$$;
COMMENT ON FUNCTION pg_catalog.master_apply_delete_command(text)
    IS 'drop shards matching delete criteria and update metadata';

CREATE FUNCTION pg_catalog.master_get_table_metadata(
                                          relation_name text,
                                          OUT logical_relid oid,
                                          OUT part_storage_type "char",
                                          OUT part_method "char", OUT part_key text,
                                          OUT part_replica_count integer,
                                          OUT part_max_size bigint,
                                          OUT part_placement_policy integer)
    RETURNS record
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$master_get_table_metadata$$;
COMMENT ON FUNCTION master_get_table_metadata(relation_name text)
    IS 'fetch metadata values for the table';
ALTER TABLE pg_catalog.pg_dist_partition DROP COLUMN autoconverted;

CREATE FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    RETURNS real
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_append_table_to_shard$$;
COMMENT ON FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    IS 'append given table to all shard placements and update metadata';

GRANT ALL ON FUNCTION start_metadata_sync_to_node(text, integer) TO PUBLIC;
GRANT ALL ON FUNCTION stop_metadata_sync_to_node(text, integer,bool) TO PUBLIC;

DROP FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool);
CREATE FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
        RETURNS void
        LANGUAGE C STRICT
        AS 'MODULE_PATHNAME', $$citus_disable_node$$;
COMMENT ON FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
        IS 'removes node from the cluster temporarily';

DROP FUNCTION pg_catalog.citus_check_connection_to_node (text, integer);
DROP FUNCTION pg_catalog.citus_check_cluster_node_health ();

DROP FUNCTION pg_catalog.citus_internal_add_object_metadata(text, text[], text[], integer, integer, boolean);
DROP FUNCTION pg_catalog.citus_run_local_command(text);
DROP FUNCTION pg_catalog.worker_drop_sequence_dependency(text);
DROP FUNCTION pg_catalog.worker_drop_shell_table(table_name text);

CREATE OR REPLACE VIEW pg_catalog.citus_shards_on_worker AS
	SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r','p','v','m','S','f','')
	      AND n.nspname <> 'pg_catalog'
	      AND n.nspname <> 'information_schema'
	      AND n.nspname !~ '^pg_toast'
          AND pg_catalog.relation_is_a_known_shard(c.oid)
	ORDER BY 1,2;

CREATE OR REPLACE VIEW pg_catalog.citus_shard_indexes_on_worker AS
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
 c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
      AND pg_catalog.relation_is_a_known_shard(c.oid)
ORDER BY 1,2;

DROP FUNCTION pg_catalog.citus_shards_on_worker();
DROP FUNCTION pg_catalog.citus_shard_indexes_on_worker();
#include "../udfs/create_distributed_function/9.0-1.sql"
ALTER TABLE citus.pg_dist_object DROP COLUMN force_delegation;
