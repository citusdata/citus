SHOW citus.enable_metadata_sync;

SELECT bool_and(metadatasynced) FROM pg_dist_node WHERE noderole = 'primary';

-- Create the necessary test utility function
CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';

-- Following tests capture the difference between the metadata in the worker nodes and the
-- coordinator. It is expected to see no rows. However if the tests fail, we list the
-- problematic queries in the activate_node_snapshot() result set.

-- list all metadata that is missing in the worker nodes
SELECT unnest(activate_node_snapshot())
    EXCEPT
SELECT unnest(result::text[]) AS unnested_result
FROM run_command_on_workers($$SELECT activate_node_snapshot()$$);

-- list all the metadata that is missing on the coordinator
SELECT unnest(result::text[]) AS unnested_result
FROM run_command_on_workers($$SELECT activate_node_snapshot()$$)
    EXCEPT
SELECT unnest(activate_node_snapshot());

SELECT pg_identify_object_as_address(classid, objid, objsubid), * from pg_catalog.pg_dist_object;
SELECT unnest(result::text[]) FROM run_command_on_workers($$select array_agg(pg_identify_object_as_address(classid, objid, objsubid)) from pg_catalog.pg_dist_object$$) order by 1;