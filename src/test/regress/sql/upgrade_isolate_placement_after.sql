-- upgrade_columnar_before renames public to citus_schema and recreates public
-- schema. But this file depends on get_colocated_shards_needisolatednode()
-- function and get_colocated_shards_needisolatednode() depends on another
-- function --get_enumerated_shard_groups()-- that is presumably created earlier
-- within the original public schema, so we temporarily rename citus_schema to
-- public here; and revert those changes at the end of this file.
ALTER SCHEMA public RENAME TO old_public;
ALTER SCHEMA citus_schema RENAME TO public;

SELECT result FROM run_command_on_all_nodes($$
    SELECT * FROM public.get_colocated_shards_needisolatednode('upgrade_isolate_placement_before.table_with_isolated_placements')
$$)
ORDER BY nodeid;

ALTER SCHEMA public RENAME TO citus_schema;
ALTER SCHEMA old_public RENAME TO public;
