CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cdbdt$
DECLARE
    constraint_event_count INTEGER;
    v_obj record;
    dropped_table_is_a_partition boolean := false;
BEGIN
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
                 WHERE object_type IN ('table', 'foreign table')
    LOOP
        -- first drop the table and metadata on the workers
        -- then drop all the shards on the workers
        -- finally remove the pg_dist_partition entry on the coordinator
        PERFORM master_remove_distributed_table_metadata_from_workers(v_obj.objid, v_obj.schema_name, v_obj.object_name);

        -- If both original and normal values are false, the dropped table was a partition
        -- that was dropped as a result of its parent being dropped
        -- NOTE: the other way around is not true:
        -- the table being a partition doesn't imply both original and normal values are false
        SELECT (v_obj.original = false AND v_obj.normal = false) INTO dropped_table_is_a_partition;

        -- The partition's shards will be dropped when dropping the parent's shards, so we can skip:
        -- i.e. we call citus_drop_all_shards with drop_shards_metadata_only parameter set to true
        IF dropped_table_is_a_partition
        THEN
            PERFORM citus_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name, drop_shards_metadata_only := true);
        ELSE
            PERFORM citus_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name, drop_shards_metadata_only := false);
        END IF;

        PERFORM master_remove_partition_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        -- Remove entries from pg_catalog.pg_dist_schema for all dropped tenant schemas.
        -- Also delete the corresponding colocation group from pg_catalog.pg_dist_colocation.
        --
        -- Although normally we automatically delete the colocation groups when they become empty,
        -- we don't do so for the colocation groups that are created for tenant schemas. For this
        -- reason, here we need to delete the colocation group when the tenant schema is dropped.
        IF v_obj.object_type = 'schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_dist_schema WHERE schemaid = v_obj.objid)
        THEN
            PERFORM pg_catalog.citus_internal_unregister_tenant_schema_globally(v_obj.objid, v_obj.object_name);
        END IF;

        -- remove entries from citus.pg_dist_object for all dropped root (objsubid = 0) objects
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
