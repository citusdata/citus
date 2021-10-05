-- citus--11.0-1--10.2-3

DROP FUNCTION pg_catalog.fix_all_partition_shard_index_names();
DROP FUNCTION pg_catalog.fix_partition_shard_index_names(regclass);
DROP FUNCTION pg_catalog.worker_fix_partition_shard_index_names(regclass, text, text);
