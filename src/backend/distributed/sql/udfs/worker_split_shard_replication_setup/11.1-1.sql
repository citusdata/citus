CREATE TYPE citus.split_shard_info AS (
    source_shard_id bigint,
    distribution_column text,
    child_shard_id bigint,
    shard_min_value text,
    shard_max_value text,
    node_id integer);
ALTER TYPE citus.split_shard_info SET SCHEMA pg_catalog;
COMMENT ON TYPE pg_catalog.split_shard_info
    IS 'Stores split child shard information';

CREATE TYPE citus.replication_slot_info AS(node_id integer, slot_owner text, slot_name text);
ALTER TYPE citus.replication_slot_info SET SCHEMA pg_catalog;
COMMENT ON TYPE pg_catalog.replication_slot_info
    IS 'Replication slot information to be used for subscriptions during non blocking shard split';

CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_replication_setup(
    splitShardInfo pg_catalog.split_shard_info[])
RETURNS setof pg_catalog.replication_slot_info
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_replication_setup(splitShardInfo pg_catalog.split_shard_info[])
    IS 'Replication setup for splitting a shard';

REVOKE ALL ON FUNCTION pg_catalog.worker_split_shard_replication_setup(pg_catalog.split_shard_info[]) FROM PUBLIC;

--- Todo(saawasek): Will change the location later by introducing new file
CREATE TABLE citus.pg_shard_cleanup(
    workflow_id text,
    object_type text,
    object_name text
);
ALTER TABLE citus.pg_shard_cleanup SET SCHEMA pg_catalog;

CREATE OR REPLACE FUNCTION pg_catalog.worker_cleanup_artifacts(
    workflow_id text,
    operation_name text)
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_cleanup_artifacts$$;
COMMENT ON FUNCTION pg_catalog.worker_cleanup_artifacts(workflow_id text, operation_name text)
    IS 'UDF to clean up artifacts';
REVOKE ALL ON FUNCTION pg_catalog.worker_cleanup_artifacts(workflow_id text, operation_name text) FROM PUBLIC;