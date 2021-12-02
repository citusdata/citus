-- citus--11.0-1--10.2-4

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
