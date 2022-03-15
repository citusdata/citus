SHOW citus.enable_metadata_sync;

SELECT bool_and(metadatasynced) FROM pg_dist_node WHERE noderole = 'primary';

-- Show that pg_dist_object entities are same on all nodes
SELECT pg_identify_object_as_address(classid, objid, objsubid)::text
FROM pg_catalog.pg_dist_object
    EXCEPT
SELECT unnest(result::text[]) AS unnested_result
FROM run_command_on_workers($$SELECT array_agg(pg_identify_object_as_address(classid, objid, objsubid)) from pg_catalog.pg_dist_object$$);
