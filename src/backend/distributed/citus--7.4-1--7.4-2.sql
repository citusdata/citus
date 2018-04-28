/* citus--7.4-1--7.4-2 */

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
        -- drop all shards and the metadata
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_drop_distributed_table_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    IF cardinality(sequence_names) = 0 THEN
        RETURN;
    END IF;

    PERFORM master_drop_sequences(sequence_names);
END;
$cdbdt$;
COMMENT ON FUNCTION pg_catalog.citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';
