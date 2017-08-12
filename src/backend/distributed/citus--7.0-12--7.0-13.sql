/* citus--7.0-12--7.0-13.sql */

SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
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
   
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects() JOIN
                               pg_dist_partition ON (logicalrelid = objid)
                 WHERE object_type IN ('table', 'foreign table')
    LOOP
        -- get colocation group
        SELECT colocationid INTO table_colocation_id FROM pg_dist_partition WHERE logicalrelid = v_obj.objid;

        -- ensure all shards are dropped
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        
        PERFORM master_drop_distributed_table_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    IF cardinality(sequence_names) = 0 THEN
        RETURN;
    END IF;
    
    PERFORM master_drop_sequences(sequence_names);
END;
$cdbdt$;

COMMENT ON FUNCTION citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';
    
RESET search_path;

