-- citus--11.0-1--10.2-3

DROP FUNCTION pg_catalog.fix_all_partition_shard_index_names();
DROP FUNCTION pg_catalog.fix_partition_shard_index_names(regclass);
DROP FUNCTION pg_catalog.worker_fix_partition_shard_index_names(regclass, text, text);

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
