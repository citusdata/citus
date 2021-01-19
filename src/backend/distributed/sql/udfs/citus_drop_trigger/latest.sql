CREATE OR REPLACE FUNCTION pg_catalog.notify_constraint_dropped()
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$notify_constraint_dropped$$;

CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cdbdt$
DECLARE
    constraint_event_count INTEGER;
    v_obj record;
    sequence_names text[] := '{}';
    table_colocation_id integer;
    propagate_drop boolean := false;
BEGIN
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
                 WHERE object_type IN ('table', 'foreign table')
    LOOP
        -- first drop the table and metadata on the workers
        -- then drop all the shards on the workers
        -- finally remove the pg_dist_partition entry on the coordinator
        PERFORM master_remove_distributed_table_metadata_from_workers(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM citus_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_remove_partition_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    -- remove entries from citus.pg_dist_object for all dropped root (objsubid = 0) objects
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        PERFORM master_unmark_object_distributed(v_obj.classid, v_obj.objid, v_obj.objsubid);
    END LOOP;

    SELECT COUNT(*) INTO constraint_event_count
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type IN ('table constraint');

    IF constraint_event_count > 0
    THEN
        -- Tell utility hook that a table constraint is dropped so we might
        -- need to undistribute some of the citus local tables that are not
        -- connected to any reference tables.
        PERFORM notify_constraint_dropped();
    END IF;
END;
$cdbdt$;
COMMENT ON FUNCTION pg_catalog.citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';
