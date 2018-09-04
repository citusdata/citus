/* citus--8.0-2--8.0-3 */
SET search_path = 'pg_catalog';

CREATE FUNCTION master_remove_partition_metadata(logicalrelid regclass,
                                                   schema_name text,
                                                   table_name text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_remove_partition_metadata$$;
COMMENT ON FUNCTION master_remove_partition_metadata(logicalrelid regclass,
                                                       schema_name text,
                                                       table_name text)
    IS 'deletes the partition metadata of a distributed table';

CREATE OR REPLACE FUNCTION master_remove_distributed_table_metadata_from_workers(logicalrelid regclass,
                                                   schema_name text,
                                                   table_name text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_remove_distributed_table_metadata_from_workers$$;
COMMENT ON FUNCTION master_remove_distributed_table_metadata_from_workers(logicalrelid regclass,
                                                       schema_name text,
                                                       table_name text)
    IS 'drops the table and removes all the metadata belonging the distributed table in the worker nodes with metadata.';


CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cdbdt$
DECLARE
    v_obj record;
    sequence_names text[] := '{}';
    table_colocation_id integer;
    propagate_drop boolean := false;
BEGIN
    -- collect set of dropped sequences to drop on workers later
    SELECT array_agg(object_identity) INTO sequence_names
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type = 'sequence';

    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
                 WHERE object_type IN ('table', 'foreign table')
    LOOP
        -- first drop the table and metadata on the workers
        -- then drop all the shards on the workers
        -- finally remove the pg_dist_partition entry on the coordinator
        PERFORM master_remove_distributed_table_metadata_from_workers(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_remove_partition_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    IF cardinality(sequence_names) = 0 THEN
        RETURN;
    END IF;

    PERFORM master_drop_sequences(sequence_names);
END;
$cdbdt$;
COMMENT ON FUNCTION pg_catalog.citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';

RESET search_path;
