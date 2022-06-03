-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION citus" to load this file. \quit

CREATE SCHEMA citus;

SET search_path = 'pg_catalog';

-- Enable SSL to encrypt all trafic by default

-- create temporary UDF that has the power to change settings within postgres and drop it
-- after ssl has been setup.
CREATE FUNCTION citus_setup_ssl()
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_setup_ssl$$;

DO LANGUAGE plpgsql
$$
BEGIN
    -- setup ssl when postgres is OpenSSL-enabled
    IF current_setting('ssl_ciphers') != 'none' THEN
        PERFORM citus_setup_ssl();
    END IF;
END;
$$;

DROP FUNCTION citus_setup_ssl();

-- Citus data types

CREATE TYPE citus.distribution_type AS ENUM (
   'hash',
   'range',
   'append'
);


-- Citus tables & corresponding indexes

CREATE TABLE citus.pg_dist_partition(
    logicalrelid regclass NOT NULL,
    partmethod "char" NOT NULL,
    partkey text,
	colocationid integer DEFAULT 0 NOT NULL,
	repmodel "char" DEFAULT 'c' NOT NULL
);
--  SELECT granted to PUBLIC in upgrade script
CREATE UNIQUE INDEX pg_dist_partition_logical_relid_index
ON citus.pg_dist_partition using btree(logicalrelid);
ALTER TABLE citus.pg_dist_partition SET SCHEMA pg_catalog;
CREATE INDEX pg_dist_partition_colocationid_index
ON pg_catalog.pg_dist_partition using btree(colocationid);

CREATE TABLE citus.pg_dist_shard(
    logicalrelid regclass NOT NULL,
    shardid int8 NOT NULL,
    shardstorage "char" NOT NULL,
    shardalias text,
    shardminvalue text,
    shardmaxvalue text
);
-- ALTER-after-CREATE to keep table tuple layout consistent
-- with earlier versions of Citus.
ALTER TABLE citus.pg_dist_shard DROP shardalias;
--  SELECT granted to PUBLIC in upgrade script
CREATE UNIQUE INDEX pg_dist_shard_shardid_index
ON citus.pg_dist_shard using btree(shardid);
CREATE INDEX pg_dist_shard_logical_relid_index
ON citus.pg_dist_shard using btree(logicalrelid);
ALTER TABLE citus.pg_dist_shard SET SCHEMA pg_catalog;

CREATE SEQUENCE citus.pg_dist_shard_placement_placementid_seq
    NO CYCLE;
ALTER SEQUENCE citus.pg_dist_shard_placement_placementid_seq
    SET SCHEMA pg_catalog;

CREATE TABLE citus.pg_dist_shard_placement(
    shardid int8 NOT NULL,
    shardstate int4 NOT NULL,
    shardlength int8 NOT NULL,
    nodename text NOT NULL,
    nodeport int8 NOT NULL,
	placementid bigint NOT NULL DEFAULT nextval('pg_catalog.pg_dist_shard_placement_placementid_seq')
);
--  SELECT granted to PUBLIC in upgrade script
CREATE UNIQUE INDEX pg_dist_shard_placement_placementid_index
ON citus.pg_dist_shard_placement using btree(placementid);
CREATE INDEX pg_dist_shard_placement_shardid_index
ON citus.pg_dist_shard_placement using btree(shardid);
CREATE INDEX pg_dist_shard_placement_nodeid_index
ON citus.pg_dist_shard_placement using btree(nodename, nodeport);
ALTER TABLE citus.pg_dist_shard_placement SET SCHEMA pg_catalog;

--  Citus sequences

-- Internal sequence to generate 64-bit shard ids. These identifiers are then
-- used to identify shards in the distributed database.

CREATE SEQUENCE citus.pg_dist_shardid_seq
    MINVALUE 102008
    NO CYCLE;
ALTER SEQUENCE  citus.pg_dist_shardid_seq SET SCHEMA pg_catalog;

-- Internal sequence to generate 32-bit jobIds. These identifiers are then
-- used to identify jobs in the distributed database; and they wrap at 32-bits
-- to allow for worker nodes to independently execute their distributed jobs.
CREATE SEQUENCE citus.pg_dist_jobid_seq
    MINVALUE 2 --  first jobId reserved for clean up jobs
    MAXVALUE 4294967296;
ALTER SEQUENCE  citus.pg_dist_jobid_seq SET SCHEMA pg_catalog;


-- Citus functions

--  master_* functions

CREATE FUNCTION master_get_table_metadata(relation_name text, OUT logical_relid oid,
                                          OUT part_storage_type "char",
                                          OUT part_method "char", OUT part_key text,
                                          OUT part_replica_count integer,
                                          OUT part_max_size bigint,
                                          OUT part_placement_policy integer)
    RETURNS record
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$master_get_table_metadata$$;
COMMENT ON FUNCTION master_get_table_metadata(relation_name text)
    IS 'fetch metadata values for the table';

CREATE FUNCTION master_get_table_ddl_events(text)
    RETURNS SETOF text
    LANGUAGE C STRICT ROWS 100
    AS 'MODULE_PATHNAME', $$master_get_table_ddl_events$$;
COMMENT ON FUNCTION master_get_table_ddl_events(text)
    IS 'fetch set of ddl statements for the table';

CREATE FUNCTION master_get_new_shardid()
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_get_new_shardid$$;
COMMENT ON FUNCTION master_get_new_shardid()
    IS 'fetch unique shardId';

CREATE FUNCTION master_create_empty_shard(text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_create_empty_shard$$;
COMMENT ON FUNCTION master_create_empty_shard(text)
    IS 'create an empty shard and shard placements for the table';

CREATE FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    RETURNS real
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_append_table_to_shard$$;
COMMENT ON FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    IS 'append given table to all shard placements and update metadata';

CREATE FUNCTION master_drop_all_shards(logicalrelid regclass,
                                       schema_name text,
                                       table_name text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_drop_all_shards$$;
COMMENT ON FUNCTION master_drop_all_shards(regclass, text, text)
    IS 'drop all shards in a relation and update metadata';

CREATE FUNCTION master_apply_delete_command(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_apply_delete_command$$;
COMMENT ON FUNCTION master_apply_delete_command(text)
    IS 'drop shards matching delete criteria and update metadata';

CREATE FUNCTION master_get_active_worker_nodes(OUT node_name text, OUT node_port bigint)
    RETURNS SETOF record
    LANGUAGE C STRICT ROWS 100
    AS 'MODULE_PATHNAME', $$master_get_active_worker_nodes$$;
COMMENT ON FUNCTION master_get_active_worker_nodes()
    IS 'fetch set of active worker nodes';

CREATE FUNCTION master_create_distributed_table(table_name regclass,
                                                distribution_column text,
                                                distribution_method citus.distribution_type)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_create_distributed_table$$;
COMMENT ON FUNCTION master_create_distributed_table(table_name regclass,
                                                    distribution_column text,
                                                    distribution_method citus.distribution_type)
    IS 'define the table distribution functions';

-- define shard creation function for hash-partitioned tables
CREATE FUNCTION master_create_worker_shards(table_name text, shard_count integer,
                                            replication_factor integer DEFAULT 2)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

--  task_tracker_* functions

CREATE FUNCTION task_tracker_assign_task(bigint, integer, text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_assign_task$$;
COMMENT ON FUNCTION task_tracker_assign_task(bigint, integer, text)
    IS 'assign a task to execute';

CREATE FUNCTION task_tracker_task_status(bigint, integer)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_task_status$$;
COMMENT ON FUNCTION task_tracker_task_status(bigint, integer)
    IS 'check an assigned task''s execution status';

CREATE FUNCTION task_tracker_cleanup_job(bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$task_tracker_cleanup_job$$;
COMMENT ON FUNCTION task_tracker_cleanup_job(bigint)
    IS 'clean up all tasks associated with a job';


--  worker_* functions

CREATE FUNCTION worker_fetch_partition_file(bigint, integer, integer, integer, text,
                                            integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_fetch_partition_file$$;
COMMENT ON FUNCTION worker_fetch_partition_file(bigint, integer, integer, integer, text,
                                                integer)
    IS 'fetch partition file from remote node';

CREATE FUNCTION worker_range_partition_table(bigint, integer, text, text, oid, anyarray)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_range_partition_table$$;
COMMENT ON FUNCTION worker_range_partition_table(bigint, integer, text, text, oid,
                                                 anyarray)
    IS 'range partition query results';

CREATE FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid, anyarray)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_hash_partition_table$$;
COMMENT ON FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid,
                                                anyarray)
    IS 'hash partition query results';

CREATE FUNCTION worker_merge_files_into_table(bigint, integer, text[], text[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_merge_files_into_table$$;
COMMENT ON FUNCTION worker_merge_files_into_table(bigint, integer, text[], text[])
    IS 'merge files into a table';

CREATE FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_merge_files_and_run_query$$;
COMMENT ON FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text)
    IS 'merge files and run a reduce query on merged files';

CREATE FUNCTION worker_cleanup_job_schema_cache()
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_cleanup_job_schema_cache$$;
COMMENT ON FUNCTION worker_cleanup_job_schema_cache()
    IS 'cleanup all job schemas in current database';

CREATE FUNCTION worker_append_table_to_shard(text, text, text, integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_append_table_to_shard$$;
COMMENT ON FUNCTION worker_append_table_to_shard(text, text, text, integer)
    IS 'append a regular table''s contents to the shard';

CREATE FUNCTION master_drop_sequences(sequence_names text[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_drop_sequences$$;
COMMENT ON FUNCTION master_drop_sequences(text[])
    IS 'drop specified sequences from the cluster';

CREATE FUNCTION master_dist_partition_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_partition_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_partition_cache_invalidate()
    IS 'register relcache invalidation for changed rows';

CREATE FUNCTION master_dist_shard_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_shard_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_shard_cache_invalidate()
    IS 'register relcache invalidation for changed rows';


--  internal functions, not user accessible

CREATE FUNCTION citus_extradata_container(INTERNAL)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_extradata_container$$;
COMMENT ON FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    IS 'placeholder function to store additional data in postgres node trees';


-- Citus triggers

CREATE TRIGGER dist_partition_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_partition
    FOR EACH ROW EXECUTE PROCEDURE master_dist_partition_cache_invalidate();

CREATE TRIGGER dist_shard_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_shard
    FOR EACH ROW EXECUTE PROCEDURE master_dist_shard_cache_invalidate();


-- Citus aggregates

DO $proc$
BEGIN
IF substring(current_Setting('server_version'), '\d+')::int >= 14 THEN
    EXECUTE $$
CREATE AGGREGATE array_cat_agg(anycompatiblearray) (SFUNC = array_cat, STYPE = anycompatiblearray);
COMMENT ON AGGREGATE array_cat_agg(anycompatiblearray)
    IS 'concatenate input arrays into a single array';
  $$;
ELSE
    EXECUTE $$
CREATE AGGREGATE array_cat_agg(anyarray) (SFUNC = array_cat, STYPE = anyarray);
COMMENT ON AGGREGATE array_cat_agg(anyarray)
    IS 'concatenate input arrays into a single array';
    $$;
END IF;
END$proc$;

GRANT SELECT ON pg_catalog.pg_dist_partition TO public;
GRANT SELECT ON pg_catalog.pg_dist_shard TO public;
GRANT SELECT ON pg_catalog.pg_dist_shard_placement TO public;

--  empty, but required to update the extension version
CREATE FUNCTION pg_catalog.master_modify_multiple_shards(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_modify_multiple_shards$$;
COMMENT ON FUNCTION master_modify_multiple_shards(text)
    IS 'push delete and update queries to shards';

CREATE FUNCTION pg_catalog.master_update_shard_statistics(shard_id bigint)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_update_shard_statistics$$;
COMMENT ON FUNCTION master_update_shard_statistics(bigint)
    IS 'updates shard statistics and returns the updated shard size';

CREATE FUNCTION pg_catalog.worker_apply_shard_ddl_command(bigint, text, text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_shard_ddl_command$$;
COMMENT ON FUNCTION worker_apply_shard_ddl_command(bigint, text, text)
    IS 'extend ddl command with shardId and apply on database';

CREATE FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_fetch_foreign_file$$;
COMMENT ON FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[])
    IS 'fetch foreign file from remote node and apply file';

CREATE FUNCTION pg_catalog.worker_apply_shard_ddl_command(bigint, text)
    RETURNS void
    LANGUAGE sql
AS $worker_apply_shard_ddl_command$
    SELECT pg_catalog.worker_apply_shard_ddl_command($1, 'public', $2);
$worker_apply_shard_ddl_command$;
COMMENT ON FUNCTION worker_apply_shard_ddl_command(bigint, text)
    IS 'extend ddl command with shardId and apply on database';

CREATE FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$shard_name$$;
COMMENT ON FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint)
    IS 'returns schema-qualified, shard-extended identifier of object name';
CREATE SEQUENCE citus.pg_dist_groupid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

CREATE SEQUENCE citus.pg_dist_node_nodeid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

ALTER SEQUENCE citus.pg_dist_groupid_seq SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_node_nodeid_seq SET SCHEMA pg_catalog;

--  add pg_dist_node
CREATE TABLE citus.pg_dist_node(
	nodeid int NOT NULL DEFAULT nextval('pg_dist_groupid_seq') PRIMARY KEY,
	groupid int NOT NULL DEFAULT nextval('pg_dist_node_nodeid_seq'),
	nodename text NOT NULL,
	nodeport int NOT NULL DEFAULT 5432,
	noderack text NOT NULL DEFAULT 'default',
	UNIQUE (nodename, nodeport)
);
-- ALTER-after-CREATE to preserve table tuple layout
ALTER TABLE citus.pg_dist_node
	ADD hasmetadata bool NOT NULL DEFAULT false,
	ADD isactive bool NOT NULL DEFAULT true;

ALTER TABLE citus.pg_dist_node SET SCHEMA pg_catalog;

CREATE FUNCTION master_dist_node_cache_invalidate()
	RETURNS trigger
	LANGUAGE C
	AS 'MODULE_PATHNAME', $$master_dist_node_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_node_cache_invalidate()
	IS 'invalidate internal cache of nodes when pg_dist_nodes changes';
CREATE TRIGGER dist_node_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_node
    FOR EACH ROW EXECUTE PROCEDURE master_dist_node_cache_invalidate();

CREATE FUNCTION master_remove_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_remove_node$$;
COMMENT ON FUNCTION master_remove_node(nodename text, nodeport integer)
	IS 'remove node from the cluster';

CREATE FUNCTION pg_catalog.master_get_new_placementid()
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_get_new_placementid$$;
COMMENT ON FUNCTION pg_catalog.master_get_new_placementid()
    IS 'fetch unique placementid';

CREATE FUNCTION pg_catalog.worker_drop_distributed_table(logicalrelid Oid)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_distributed_table$$;

COMMENT ON FUNCTION pg_catalog.worker_drop_distributed_table(logicalrelid Oid)
    IS 'drop the clustered table and its reference from metadata tables';

CREATE FUNCTION pg_catalog.column_name_to_column(table_name regclass, column_name text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_name_to_column$$;
COMMENT ON FUNCTION pg_catalog.column_name_to_column(table_name regclass, column_name text)
    IS 'convert a column name to its textual Var representation';

CREATE FUNCTION pg_catalog.get_colocated_table_array(regclass)
    RETURNS regclass[]
    AS 'citus'
    LANGUAGE C STRICT;

CREATE TABLE citus.pg_dist_local_group(
    groupid int NOT NULL PRIMARY KEY)
;

--  insert the default value for being the coordinator node
INSERT INTO citus.pg_dist_local_group VALUES (0);

ALTER TABLE citus.pg_dist_local_group SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_local_group TO public;

CREATE TABLE citus.pg_dist_transaction (
    groupid int NOT NULL,
    gid text NOT NULL
);

CREATE INDEX pg_dist_transaction_group_index
ON citus.pg_dist_transaction using btree(groupid);

ALTER TABLE citus.pg_dist_transaction SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.pg_dist_transaction
ADD CONSTRAINT pg_dist_transaction_unique_constraint UNIQUE (groupid, gid);

GRANT SELECT ON pg_catalog.pg_dist_transaction TO public;

CREATE FUNCTION pg_catalog.recover_prepared_transactions()
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$recover_prepared_transactions$$;

COMMENT ON FUNCTION pg_catalog.recover_prepared_transactions()
    IS 'recover prepared transactions started by this node';

CREATE SEQUENCE citus.pg_dist_colocationid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

ALTER SEQUENCE citus.pg_dist_colocationid_seq SET SCHEMA pg_catalog;

--  add pg_dist_colocation
CREATE TABLE citus.pg_dist_colocation(
	colocationid int NOT NULL PRIMARY KEY,
	shardcount int NOT NULL,
	replicationfactor int NOT NULL,
	distributioncolumntype oid NOT NULL
);

ALTER TABLE citus.pg_dist_colocation SET SCHEMA pg_catalog;

CREATE INDEX pg_dist_colocation_configuration_index
ON pg_dist_colocation USING btree(shardcount, replicationfactor, distributioncolumntype);

CREATE FUNCTION create_reference_table(table_name regclass)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$create_reference_table$$;
COMMENT ON FUNCTION create_reference_table(table_name regclass)
	IS 'create a distributed reference table';

CREATE FUNCTION pg_catalog.worker_apply_inter_shard_ddl_command(referencing_shard bigint,
																referencing_schema_name text,
																referenced_shard bigint,
																referenced_schema_name text,
																command text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_inter_shard_ddl_command$$;
COMMENT ON FUNCTION pg_catalog.worker_apply_inter_shard_ddl_command(referencing_shard bigint,
																	referencing_schema_name text,
																	referenced_shard bigint,
																	referenced_schema_name text,
																	command text)
    IS 'executes inter shard ddl command';

CREATE FUNCTION pg_catalog.master_dist_placement_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_placement_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_placement_cache_invalidate()
    IS 'register relcache invalidation for changed placements';

CREATE TRIGGER dist_placement_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_shard_placement
    FOR EACH ROW EXECUTE PROCEDURE master_dist_placement_cache_invalidate();

CREATE FUNCTION mark_tables_colocated(source_table_name regclass, target_table_names regclass[])
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$mark_tables_colocated$$;
COMMENT ON FUNCTION mark_tables_colocated(source_table_name regclass, target_table_names regclass[])
	IS 'mark target distributed tables as colocated with the source table';

CREATE FUNCTION start_metadata_sync_to_node(nodename text, nodeport integer)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$start_metadata_sync_to_node$$;
COMMENT ON FUNCTION start_metadata_sync_to_node(nodename text, nodeport integer)
    IS 'sync metadata to node';

CREATE FUNCTION worker_create_truncate_trigger(table_name regclass)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$worker_create_truncate_trigger$$;
COMMENT ON FUNCTION worker_create_truncate_trigger(tablename regclass)
	IS 'create truncate trigger for distributed table';

CREATE FUNCTION stop_metadata_sync_to_node(nodename text, nodeport integer)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$stop_metadata_sync_to_node$$;
COMMENT ON FUNCTION stop_metadata_sync_to_node(nodename text, nodeport integer)
    IS 'stop metadata sync to node';

CREATE FUNCTION column_to_column_name(table_name regclass, column_var_text text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_to_column_name$$;
COMMENT ON FUNCTION column_to_column_name(table_name regclass, column_var_text text)
    IS 'convert the textual Var representation to a column name';

CREATE FUNCTION create_distributed_table(table_name regclass,
										 distribution_column text,
										 distribution_type citus.distribution_type DEFAULT 'hash',
										 colocate_with text DEFAULT 'default')
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$create_distributed_table$$;
COMMENT ON FUNCTION create_distributed_table(table_name regclass,
											 distribution_column text,
											 distribution_type citus.distribution_type,
											 colocate_with text)
    IS 'creates a distributed table';

CREATE FUNCTION get_shard_id_for_distribution_column(table_name regclass, distribution_value "any" DEFAULT NULL)
	RETURNS bigint
	LANGUAGE C
	AS 'MODULE_PATHNAME', $$get_shard_id_for_distribution_column$$;
COMMENT ON FUNCTION get_shard_id_for_distribution_column(table_name regclass, distribution_value "any")
    IS 'return shard id which belongs to given table and contains given value';

CREATE FUNCTION lock_shard_resources(lock_mode int, shard_id bigint[])
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$lock_shard_resources$$;
COMMENT ON FUNCTION lock_shard_resources(lock_mode int, shard_id bigint[])
    IS 'lock shard resource to serialise non-commutative writes';

CREATE FUNCTION lock_shard_metadata(lock_mode int, shard_id bigint[])
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$lock_shard_metadata$$;
COMMENT ON FUNCTION lock_shard_metadata(lock_mode int, shard_id bigint[])
    IS 'lock shard metadata to prevent writes during metadata changes';

CREATE FUNCTION master_drop_distributed_table_metadata(logicalrelid regclass,
                                          			   schema_name text,
                                          			   table_name text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_drop_distributed_table_metadata$$;
COMMENT ON FUNCTION master_drop_distributed_table_metadata(logicalrelid regclass,
                                              			   schema_name text,
                                              			   table_name text)
    IS 'delete metadata of the distributed table';

-- allow users to read catalog tables
GRANT SELECT ON pg_catalog.pg_dist_node TO public;
GRANT SELECT ON pg_catalog.pg_dist_colocation TO public;
GRANT SELECT ON pg_catalog.pg_dist_colocationid_seq TO public;
GRANT SELECT ON pg_catalog.pg_dist_groupid_seq TO public;
GRANT SELECT ON pg_catalog.pg_dist_node_nodeid_seq TO public;
GRANT SELECT ON pg_catalog.pg_dist_shard_placement_placementid_seq TO public;
GRANT SELECT ON pg_catalog.pg_dist_shardid_seq TO public;
GRANT SELECT ON pg_catalog.pg_dist_jobid_seq TO public;

CREATE FUNCTION upgrade_to_reference_table(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$upgrade_to_reference_table$$;
COMMENT ON FUNCTION upgrade_to_reference_table(table_name regclass)
    IS 'upgrades an existing broadcast table to a reference table';

CREATE FUNCTION master_disable_node(nodename text, nodeport integer)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_disable_node$$;
COMMENT ON FUNCTION master_disable_node(nodename text, nodeport integer)
	IS 'removes node from the cluster temporarily';

-- functions for running commands on workers and shards
CREATE FUNCTION pg_catalog.master_run_on_worker(worker_name text[],
														   port integer[],
														   command text[],
														   parallel boolean,
														   OUT node_name text,
														   OUT node_port integer,
														   OUT success boolean,
														   OUT result text )
	RETURNS SETOF record
	LANGUAGE C STABLE STRICT
	AS 'MODULE_PATHNAME', $$master_run_on_worker$$;


CREATE TYPE citus.colocation_placement_type AS (
    shardid1 bigint,
    shardid2 bigint,
    nodename text,
    nodeport bigint
);

--
-- distributed_tables_colocated returns true if given tables are co-located, false otherwise.
-- The function checks shard definitions, matches shard placements for given tables.
--
CREATE FUNCTION pg_catalog.distributed_tables_colocated(table1 regclass,
																   table2 regclass)
    RETURNS bool
    LANGUAGE plpgsql
    AS $function$
DECLARE
	colocated_shard_count int;
	table1_shard_count int;
	table2_shard_count int;
	table1_placement_count int;
	table2_placement_count int;
	table1_placements citus.colocation_placement_type[];
	table2_placements citus.colocation_placement_type[];
BEGIN
	SELECT count(*),
	    (SELECT count(*) FROM pg_dist_shard a WHERE a.logicalrelid = table1),
	    (SELECT count(*) FROM pg_dist_shard b WHERE b.logicalrelid = table2)
	INTO colocated_shard_count, table1_shard_count, table2_shard_count
	FROM pg_dist_shard tba JOIN pg_dist_shard tbb USING(shardminvalue, shardmaxvalue)
	WHERE tba.logicalrelid = table1 AND tbb.logicalrelid = table2;

	IF (table1_shard_count != table2_shard_count OR
		table1_shard_count != colocated_shard_count)
	THEN
		RETURN false;
	END IF;

	WITH colocated_shards AS (
		SELECT tba.shardid as shardid1, tbb.shardid as shardid2
		FROM pg_dist_shard tba JOIN pg_dist_shard tbb USING(shardminvalue, shardmaxvalue)
		WHERE tba.logicalrelid = table1 AND tbb.logicalrelid = table2),
	left_shard_placements AS (
		SELECT cs.shardid1, cs.shardid2, sp.nodename, sp.nodeport
		FROM colocated_shards cs JOIN pg_dist_shard_placement sp
		ON (cs.shardid1 = sp.shardid)
		WHERE sp.shardstate = 1)
	SELECT
		array_agg(
			(lsp.shardid1, lsp.shardid2, lsp.nodename, lsp.nodeport)::citus.colocation_placement_type
			ORDER BY shardid1, shardid2, nodename, nodeport),
		count(distinct lsp.shardid1)
	FROM left_shard_placements lsp
	INTO table1_placements, table1_placement_count;

	WITH colocated_shards AS (
		SELECT tba.shardid as shardid1, tbb.shardid as shardid2
		FROM pg_dist_shard tba JOIN pg_dist_shard tbb USING(shardminvalue, shardmaxvalue)
		WHERE tba.logicalrelid = table1 AND tbb.logicalrelid = table2),
	right_shard_placements AS (
		SELECT cs.shardid1, cs.shardid2, sp.nodename, sp.nodeport
		FROM colocated_shards cs LEFT JOIN pg_dist_shard_placement sp ON(cs.shardid2 = sp.shardid)
		WHERE sp.shardstate = 1)
	SELECT
		array_agg(
			(rsp.shardid1, rsp.shardid2, rsp.nodename, rsp.nodeport)::citus.colocation_placement_type
			ORDER BY shardid1, shardid2, nodename, nodeport),
		count(distinct rsp.shardid2)
	FROM right_shard_placements rsp
	INTO table2_placements, table2_placement_count;

	IF (table1_shard_count != table1_placement_count
		OR table1_placement_count != table2_placement_count) THEN
		RETURN false;
	END IF;

	IF (array_length(table1_placements, 1) != array_length(table2_placements, 1)) THEN
		RETURN false;
	END IF;

	FOR i IN  1..array_length(table1_placements,1) LOOP
		IF (table1_placements[i].nodename != table2_placements[i].nodename OR
			table1_placements[i].nodeport != table2_placements[i].nodeport) THEN
			RETURN false;
		END IF;
	END LOOP;

	RETURN true;
END;
$function$;


CREATE FUNCTION pg_catalog.run_command_on_workers(command text,
													parallel bool default true,
													OUT nodename text,
													OUT nodeport int,
													OUT success bool,
													OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	commands text[];
BEGIN
	WITH citus_workers AS (
		SELECT * FROM master_get_active_worker_nodes() ORDER BY node_name, node_port)
	SELECT array_agg(node_name), array_agg(node_port), array_agg(command)
	INTO workers, ports, commands
	FROM citus_workers;

	RETURN QUERY SELECT * FROM master_run_on_worker(workers, ports, commands, parallel);
END;
$function$;


CREATE FUNCTION pg_catalog.run_command_on_placements(table_name regclass,
																command text,
																parallel bool default true,
																OUT nodename text,
																OUT nodeport int,
																OUT shardid bigint,
																OUT success bool,
																OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards bigint[];
	commands text[];
BEGIN
	WITH citus_placements AS (
		SELECT
			ds.logicalrelid::regclass AS tablename,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			dsp.nodename AS nodename, dsp.nodeport::int AS nodeport
		FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and ds.logicalrelid::regclass = table_name
		ORDER BY ds.logicalrelid, ds.shardid, dsp.nodename, dsp.nodeport)
	SELECT
		array_agg(cp.nodename), array_agg(cp.nodeport), array_agg(cp.shardid),
		array_agg(format(command, cp.shardname))
	INTO workers, ports, shards, commands
	FROM citus_placements cp;

	RETURN QUERY
		SELECT r.node_name, r.node_port, shards[ordinality],
			r.success, r.result
		FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;


CREATE FUNCTION pg_catalog.run_command_on_colocated_placements(
																 table_name1 regclass,
																 table_name2 regclass,
																 command text,
																 parallel bool default true,
																 OUT nodename text,
																 OUT nodeport int,
																 OUT shardid1 bigint,
																 OUT shardid2 bigint,
																 OUT success bool,
																 OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards1 bigint[];
	shards2 bigint[];
	commands text[];
BEGIN
	IF NOT (SELECT distributed_tables_colocated(table_name1, table_name2)) THEN
		RAISE EXCEPTION 'tables % and % are not co-located', table_name1, table_name2;
	END IF;

	WITH active_shard_placements AS (
		SELECT
			ds.logicalrelid,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			ds.shardminvalue AS shardminvalue,
			ds.shardmaxvalue AS shardmaxvalue,
			dsp.nodename AS nodename,
			dsp.nodeport::int AS nodeport
		FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and (ds.logicalrelid::regclass = table_name1 or
			ds.logicalrelid::regclass = table_name2)
		ORDER BY ds.logicalrelid, ds.shardid, dsp.nodename, dsp.nodeport),
	citus_colocated_placements AS (
		SELECT
			a.logicalrelid::regclass AS tablename1,
			a.shardid AS shardid1,
			shard_name(a.logicalrelid, a.shardid) AS shardname1,
			b.logicalrelid::regclass AS tablename2,
			b.shardid AS shardid2,
			shard_name(b.logicalrelid, b.shardid) AS shardname2,
			a.nodename AS nodename,
			a.nodeport::int AS nodeport
		FROM
			active_shard_placements a, active_shard_placements b
		WHERE
			a.shardminvalue = b.shardminvalue AND
			a.shardmaxvalue = b.shardmaxvalue AND
			a.logicalrelid != b.logicalrelid AND
			a.nodename = b.nodename AND
			a.nodeport = b.nodeport AND
			a.logicalrelid::regclass = table_name1 AND
			b.logicalrelid::regclass = table_name2
		ORDER BY a.logicalrelid, a.shardid, nodename, nodeport)
	SELECT
		array_agg(cp.nodename), array_agg(cp.nodeport), array_agg(cp.shardid1),
		array_agg(cp.shardid2), array_agg(format(command, cp.shardname1, cp.shardname2))
	INTO workers, ports, shards1, shards2, commands
  	FROM citus_colocated_placements cp;

	RETURN QUERY SELECT r.node_name, r.node_port, shards1[ordinality],
		shards2[ordinality], r.success, r.result
	FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;


CREATE FUNCTION pg_catalog.run_command_on_shards(table_name regclass,
															command text,
															parallel bool default true,
															OUT shardid bigint,
															OUT success bool,
															OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	workers text[];
	ports int[];
	shards bigint[];
	commands text[];
	shard_count int;
BEGIN
	SELECT COUNT(*) INTO shard_count FROM pg_dist_shard
	WHERE logicalrelid = table_name;

	WITH citus_shards AS (
		SELECT ds.logicalrelid::regclass AS tablename,
			ds.shardid AS shardid,
			shard_name(ds.logicalrelid, ds.shardid) AS shardname,
			array_agg(dsp.nodename) AS nodenames,
			array_agg(dsp.nodeport) AS nodeports
		FROM pg_dist_shard ds LEFT JOIN pg_dist_shard_placement dsp USING (shardid)
		WHERE dsp.shardstate = 1 and ds.logicalrelid::regclass = table_name
		GROUP BY ds.logicalrelid, ds.shardid
		ORDER BY ds.logicalrelid, ds.shardid)
	SELECT
		array_agg(cs.nodenames[1]), array_agg(cs.nodeports[1]), array_agg(cs.shardid),
		array_agg(format(command, cs.shardname))
	INTO workers, ports, shards, commands
	FROM citus_shards cs;

	IF (shard_count != array_length(workers, 1)) THEN
		RAISE NOTICE 'some shards do  not have active placements';
	END IF;

	RETURN QUERY
		SELECT shards[ordinality], r.success, r.result
		FROM master_run_on_worker(workers, ports, commands, parallel) WITH ORDINALITY r;
END;
$function$;

CREATE FUNCTION master_dist_local_group_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_local_group_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_local_group_cache_invalidate()
    IS 'register node cache invalidation for changed rows';

CREATE TRIGGER dist_local_group_cache_invalidate
    AFTER UPDATE
    ON pg_catalog.pg_dist_local_group
    FOR EACH ROW EXECUTE PROCEDURE master_dist_local_group_cache_invalidate();

CREATE FUNCTION worker_apply_sequence_command(text)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_apply_sequence_command$$;
COMMENT ON FUNCTION worker_apply_sequence_command(text)
    IS 'create a sequence which products globally unique values';

CREATE FUNCTION isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text DEFAULT '')
	RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$isolate_tenant_to_new_shard$$;
COMMENT ON FUNCTION isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text)
    IS 'isolate a tenant to its own shard and return the new shard id';

CREATE FUNCTION worker_hash(value "any")
	RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_hash$$;
COMMENT ON FUNCTION worker_hash(value "any")
    IS 'calculate hashed value and return it';

-- table size functions
CREATE FUNCTION citus_table_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_table_size$$;
COMMENT ON FUNCTION citus_table_size(logicalrelid regclass)
    IS 'get disk space used by the specified table, excluding indexes';

CREATE FUNCTION citus_relation_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_relation_size$$;
COMMENT ON FUNCTION citus_relation_size(logicalrelid regclass)
    IS 'get disk space used by the ''main'' fork';

CREATE FUNCTION citus_total_relation_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_total_relation_size$$;
COMMENT ON FUNCTION citus_total_relation_size(logicalrelid regclass)
    IS 'get total disk space used by the specified table';

CREATE FUNCTION pg_catalog.citus_truncate_trigger()
    RETURNS trigger
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_truncate_trigger$$;
COMMENT ON FUNCTION pg_catalog.citus_truncate_trigger()
    IS 'trigger function called when truncating the distributed table';

-- introduce replication-agnostic pg_dist_placement table
ALTER SEQUENCE pg_catalog.pg_dist_shard_placement_placementid_seq
  RENAME TO pg_dist_placement_placementid_seq;

ALTER TABLE pg_catalog.pg_dist_shard_placement
  ALTER COLUMN placementid SET DEFAULT nextval('pg_catalog.pg_dist_placement_placementid_seq');

CREATE TABLE citus.pg_dist_placement (
  placementid BIGINT NOT NULL default nextval('pg_dist_placement_placementid_seq'::regclass),
  shardid BIGINT NOT NULL,
  shardstate INT NOT NULL,
  shardlength BIGINT NOT NULL,
  groupid INT NOT NULL
);
ALTER TABLE citus.pg_dist_placement SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_placement TO public;

CREATE INDEX pg_dist_placement_groupid_index
  ON pg_dist_placement USING btree(groupid);

CREATE INDEX pg_dist_placement_shardid_index
  ON pg_dist_placement USING btree(shardid);

CREATE UNIQUE INDEX pg_dist_placement_placementid_index
  ON pg_dist_placement USING btree(placementid);

CREATE OR REPLACE FUNCTION citus.find_groupid_for_node(text, int)
RETURNS int AS $$
DECLARE
  groupid int := (SELECT groupid FROM pg_dist_node WHERE nodename = $1 AND nodeport = $2);
BEGIN
  IF groupid IS NULL THEN
    RAISE EXCEPTION 'There is no node at "%:%"', $1, $2;
  ELSE
    RETURN groupid;
  END IF;
END;
$$ LANGUAGE plpgsql;

INSERT INTO pg_catalog.pg_dist_placement
SELECT placementid, shardid, shardstate, shardlength,
       citus.find_groupid_for_node(placement.nodename, placement.nodeport::int) AS groupid
FROM pg_dist_shard_placement placement;

DROP TRIGGER dist_placement_cache_invalidate ON pg_catalog.pg_dist_shard_placement;
CREATE TRIGGER dist_placement_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_placement
    FOR EACH ROW EXECUTE PROCEDURE master_dist_placement_cache_invalidate();

-- this should be removed when noderole is added but for now it ensures the below view
-- returns the correct results and that placements unambiguously belong to a view
ALTER TABLE pg_catalog.pg_dist_node ADD CONSTRAINT pg_dist_node_groupid_unique
  UNIQUE (groupid);

DROP TABLE pg_dist_shard_placement;
CREATE VIEW citus.pg_dist_shard_placement AS
SELECT shardid, shardstate, shardlength, nodename, nodeport, placementid
-- assumes there's only one node per group
FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
  placement.groupid = node.groupid
);
ALTER VIEW citus.pg_dist_shard_placement SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_shard_placement TO public;

-- add some triggers which make it look like pg_dist_shard_placement is still a table

ALTER VIEW pg_catalog.pg_dist_shard_placement
  ALTER placementid SET DEFAULT nextval('pg_dist_placement_placementid_seq');

CREATE OR REPLACE FUNCTION citus.pg_dist_shard_placement_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      DELETE FROM pg_dist_placement WHERE placementid = OLD.placementid;
      RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
      UPDATE pg_dist_placement
        SET shardid = NEW.shardid, shardstate = NEW.shardstate,
            shardlength = NEW.shardlength, placementid = NEW.placementid,
            groupid = citus.find_groupid_for_node(NEW.nodename, NEW.nodeport)
        WHERE placementid = OLD.placementid;
      RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
      INSERT INTO pg_dist_placement
        (placementid, shardid, shardstate, shardlength, groupid)
      VALUES (NEW.placementid, NEW.shardid, NEW.shardstate, NEW.shardlength,
        citus.find_groupid_for_node(NEW.nodename, NEW.nodeport));
      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_dist_shard_placement_trigger
  INSTEAD OF INSERT OR UPDATE OR DELETE ON pg_dist_shard_placement
  FOR EACH ROW EXECUTE PROCEDURE citus.pg_dist_shard_placement_trigger_func();

CREATE TYPE pg_catalog.noderole AS ENUM (
  'primary',     -- node is available and accepting writes
  'secondary',   -- node is available but only accepts reads
  'unavailable' -- node is in recovery or otherwise not usable
-- adding new values to a type inside of a transaction (such as during an ALTER EXTENSION
-- citus UPDATE) isn't allowed in PG 9.6, and only allowed in PG10 if you don't use the
-- new values inside of the same transaction. You might need to replace this type with a
-- new one and then change the column type in pg_dist_node. There's a list of
-- alternatives here:
-- https://stackoverflow.com/questions/1771543/postgresql-updating-an-enum-type/41696273
);

ALTER TABLE pg_dist_node ADD COLUMN noderole noderole NOT NULL DEFAULT 'primary';

-- we're now allowed to have more than one node per group
ALTER TABLE pg_catalog.pg_dist_node DROP CONSTRAINT pg_dist_node_groupid_unique;

-- so make sure pg_dist_shard_placement only returns writable placements
CREATE OR REPLACE VIEW pg_catalog.pg_dist_shard_placement AS
  SELECT shardid, shardstate, shardlength, nodename, nodeport, placementid
  FROM pg_dist_placement placement INNER JOIN pg_dist_node node ON (
    placement.groupid = node.groupid AND node.noderole = 'primary'
  );

CREATE OR REPLACE FUNCTION citus.pg_dist_node_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    -- AddNodeMetadata also takes out a ShareRowExclusiveLock
    LOCK TABLE pg_dist_node IN SHARE ROW EXCLUSIVE MODE;
    IF (TG_OP = 'INSERT') THEN
      IF NEW.noderole = 'primary'
          AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
                                                       noderole = 'primary' AND
                                                       nodeid <> NEW.nodeid) THEN
        RAISE EXCEPTION 'there cannot be two primary nodes in a group';
      END IF;
      RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
      IF NEW.noderole = 'primary'
           AND EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = NEW.groupid AND
                                                        noderole = 'primary' AND
                                                        nodeid <> NEW.nodeid) THEN
         RAISE EXCEPTION 'there cannot be two primary nodes in a group';
      END IF;
      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_dist_node_trigger
  BEFORE INSERT OR UPDATE ON pg_dist_node
  FOR EACH ROW EXECUTE PROCEDURE citus.pg_dist_node_trigger_func();

-- distributed deadlocks
CREATE FUNCTION assign_distributed_transaction_id(initiator_node_identifier int4, transaction_number int8, transaction_stamp timestamptz)
     RETURNS void
     LANGUAGE C STRICT
     AS 'MODULE_PATHNAME',$$assign_distributed_transaction_id$$;
 COMMENT ON FUNCTION assign_distributed_transaction_id(initiator_node_identifier int4, transaction_number int8, transaction_stamp timestamptz)
     IS 'Only intended for internal use, users should not call this. The function sets the distributed transaction id';

CREATE OR REPLACE FUNCTION get_current_transaction_id(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     RETURNS RECORD
     LANGUAGE C STRICT
     AS 'MODULE_PATHNAME',$$get_current_transaction_id$$;
 COMMENT ON FUNCTION get_current_transaction_id(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns the current backend data including distributed transaction id';

CREATE OR REPLACE FUNCTION get_all_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
	RETURNS SETOF RECORD
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$get_all_active_transactions$$;
 COMMENT ON FUNCTION get_all_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns distributed transaction ids of active distributed transactions';

CREATE OR REPLACE FUNCTION check_distributed_deadlocks()
RETURNS BOOL
LANGUAGE 'c' STRICT
AS $$MODULE_PATHNAME$$, $$check_distributed_deadlocks$$;
COMMENT ON FUNCTION check_distributed_deadlocks()
IS 'does a distributed deadlock check, if a deadlock found cancels one of the participating backends and returns true ';

CREATE FUNCTION pg_catalog.dump_local_wait_edges(
                    OUT waiting_pid int4,
                    OUT waiting_node_id int4,
                    OUT waiting_transaction_num int8,
                    OUT waiting_transaction_stamp timestamptz,
                    OUT blocking_pid int4,
                    OUT blocking_node_id int4,
                    OUT blocking_transaction_num int8,
                    OUT blocking_transaction_stamp timestamptz,
                    OUT blocking_transaction_waiting bool)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS $$MODULE_PATHNAME$$, $$dump_local_wait_edges$$;
COMMENT ON FUNCTION pg_catalog.dump_local_wait_edges()
IS 'returns all local lock wait chains, that start from distributed transactions';

CREATE FUNCTION pg_catalog.dump_global_wait_edges(
                    OUT waiting_pid int4,
                    OUT waiting_node_id int4,
                    OUT waiting_transaction_num int8,
                    OUT waiting_transaction_stamp timestamptz,
                    OUT blocking_pid int4,
                    OUT blocking_node_id int4,
                    OUT blocking_transaction_num int8,
                    OUT blocking_transaction_stamp timestamptz,
                    OUT blocking_transaction_waiting bool)
RETURNS SETOF RECORD
LANGUAGE 'c' STRICT
AS $$MODULE_PATHNAME$$, $$dump_global_wait_edges$$;
COMMENT ON FUNCTION pg_catalog.dump_global_wait_edges()
IS 'returns a global list of blocked transactions originating from this node';

CREATE FUNCTION citus.replace_isolation_tester_func()
RETURNS void AS $$
  DECLARE
    version integer := current_setting('server_version_num');
  BEGIN
    IF version >= 100000 THEN
      ALTER FUNCTION pg_catalog.pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO old_pg_isolation_test_session_is_blocked;
      ALTER FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO pg_isolation_test_session_is_blocked;
    ELSE
      ALTER FUNCTION pg_catalog.pg_blocking_pids(integer)
        RENAME TO old_pg_blocking_pids;
      ALTER FUNCTION pg_catalog.citus_blocking_pids(integer)
        RENAME TO pg_blocking_pids;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.restore_isolation_tester_func()
RETURNS void AS $$
  DECLARE
    version integer := current_setting('server_version_num');
  BEGIN
    IF version >= 100000 THEN
      ALTER FUNCTION pg_catalog.pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO citus_isolation_test_session_is_blocked;
      ALTER FUNCTION pg_catalog.old_pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO pg_isolation_test_session_is_blocked;
    ELSE
      ALTER FUNCTION pg_catalog.pg_blocking_pids(integer)
        RENAME TO citus_blocking_pids;
      ALTER FUNCTION pg_catalog.old_pg_blocking_pids(integer)
        RENAME TO pg_blocking_pids;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.refresh_isolation_tester_prepared_statement()
RETURNS void AS $$
  BEGIN
    -- isolation creates a prepared statement using the old function before tests have a
    -- chance to call replace_isolation_tester_func. By calling that prepared statement
    -- with a different search_path we force a re-parse which picks up the new function
    SET search_path TO 'citus';
    EXECUTE 'EXECUTE isolationtester_waiting (0)';
    RESET search_path;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_catalog.citus_blocking_pids(pBlockedPid integer)
RETURNS int4[] AS $$
  DECLARE
    mLocalBlockingPids int4[];
    mRemoteBlockingPids int4[];
    mLocalTransactionNum int8;
  BEGIN
    SELECT pg_catalog.old_pg_blocking_pids(pBlockedPid) INTO mLocalBlockingPids;

    IF (array_length(mLocalBlockingPids, 1) > 0) THEN
      RETURN mLocalBlockingPids;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    SELECT transaction_number INTO mLocalTransactionNum
      FROM get_all_active_transactions() WHERE process_id = pBlockedPid;

    SELECT array_agg(process_id) INTO mRemoteBlockingPids FROM (
      WITH activeTransactions AS (
        SELECT process_id, transaction_number FROM get_all_active_transactions()
      ), blockingTransactions AS (
        SELECT blocking_transaction_num AS txn_num FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mLocalTransactionNum
      )
      SELECT activeTransactions.process_id FROM activeTransactions, blockingTransactions
      WHERE activeTransactions.transaction_number = blockingTransactions.txn_num
    ) AS sub;

    RETURN mRemoteBlockingPids;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedTransactionNum int8;
  BEGIN
    IF pg_catalog.old_pg_isolation_test_session_is_blocked(pBlockedPid, pInterestingPids) THEN
      RETURN true;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    SELECT transaction_number INTO mBlockedTransactionNum
      FROM get_all_active_transactions() WHERE process_id = pBlockedPid;

    RETURN EXISTS (
      SELECT 1 FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mBlockedTransactionNum
    );
  END;
$$ LANGUAGE plpgsql;

ALTER TABLE pg_dist_node ADD COLUMN nodecluster name NOT NULL DEFAULT 'default';
ALTER TABLE pg_dist_node
  ADD CONSTRAINT primaries_are_only_allowed_in_the_default_cluster
  CHECK (NOT (nodecluster <> 'default' AND noderole = 'primary'));

CREATE FUNCTION master_add_node(nodename text,
                                nodeport integer,
                                groupid integer default 0,
                                noderole noderole default 'primary',
                                nodecluster name default 'default',
                                OUT nodeid integer,
                                OUT groupid integer,
                                OUT nodename text,
                                OUT nodeport integer,
                                OUT noderack text,
                                OUT hasmetadata boolean,
                                OUT isactive bool,
                                OUT noderole noderole,
                                OUT nodecluster name)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_node$$;
COMMENT ON FUNCTION master_add_node(nodename text, nodeport integer,
                                    groupid integer, noderole noderole, nodecluster name)
  IS 'add node to the cluster';

CREATE FUNCTION master_add_inactive_node(nodename text,
                                         nodeport integer,
                                         groupid integer default 0,
                                         noderole noderole default 'primary',
                                         nodecluster name default 'default',
                                         OUT nodeid integer,
                                         OUT groupid integer,
                                         OUT nodename text,
                                         OUT nodeport integer,
                                         OUT noderack text,
                                         OUT hasmetadata boolean,
                                         OUT isactive bool,
                                         OUT noderole noderole,
                                         OUT nodecluster name)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME',$$master_add_inactive_node$$;
COMMENT ON FUNCTION master_add_inactive_node(nodename text,nodeport integer,
                                             groupid integer, noderole noderole,
                                             nodecluster name)
  IS 'prepare node by adding it to pg_dist_node';

CREATE FUNCTION master_activate_node(nodename text,
                                     nodeport integer,
                                     OUT nodeid integer,
                                     OUT groupid integer,
                                     OUT nodename text,
                                     OUT nodeport integer,
                                     OUT noderack text,
                                     OUT hasmetadata boolean,
                                     OUT isactive bool,
                                     OUT noderole noderole,
                                     OUT nodecluster name)
    RETURNS record
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$master_activate_node$$;
COMMENT ON FUNCTION master_activate_node(nodename text, nodeport integer)
    IS 'activate a node which is in the cluster';

CREATE FUNCTION master_add_secondary_node(nodename text,
                                          nodeport integer,
                                          primaryname text,
                                          primaryport integer,
                                          nodecluster name default 'default',
                                          OUT nodeid integer,
                                          OUT groupid integer,
                                          OUT nodename text,
                                          OUT nodeport integer,
                                          OUT noderack text,
                                          OUT hasmetadata boolean,
                                          OUT isactive bool,
                                          OUT noderole noderole,
                                          OUT nodecluster name)
  RETURNS record
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_add_secondary_node$$;
COMMENT ON FUNCTION master_add_secondary_node(nodename text, nodeport integer,
                                              primaryname text, primaryport integer,
                                              nodecluster name)
  IS 'add a secondary node to the cluster';

CREATE FUNCTION master_update_node(node_id int,
                                              new_node_name text,
                                              new_node_port int)
  RETURNS void
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$master_update_node$$;
COMMENT ON FUNCTION master_update_node(node_id int, new_node_name text, new_node_port int)
  IS 'change the location of a node';

-- shard statistics
CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
RETURNS VOID AS $$
DECLARE
	colocated_tables regclass[];
BEGIN
	SELECT get_colocated_table_array(relation) INTO colocated_tables;

	PERFORM
		master_update_shard_statistics(shardid)
	FROM
		pg_dist_shard
	WHERE
		logicalrelid = ANY (colocated_tables);
END;
$$ LANGUAGE 'plpgsql';
COMMENT ON FUNCTION master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table and its colocated tables';

CREATE OR REPLACE FUNCTION get_colocated_shard_array(bigint)
	RETURNS BIGINT[]
	LANGUAGE C STRICT
	AS 'citus', $$get_colocated_shard_array$$;
COMMENT ON FUNCTION get_colocated_shard_array(bigint)
	IS 'returns the array of colocated shards of the given shard';

-- distributed backups
CREATE OR REPLACE FUNCTION pg_catalog.citus_create_restore_point(text)
RETURNS pg_lsn
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_create_restore_point$$;
COMMENT ON FUNCTION pg_catalog.citus_create_restore_point(text)
IS 'temporarily block writes and create a named restore point on all nodes';

-- functions for giving node a unique identifier
CREATE OR REPLACE FUNCTION pg_catalog.citus_version()
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$citus_version$$;
COMMENT ON FUNCTION pg_catalog.citus_version()
    IS 'Citus version string';

CREATE TABLE citus.pg_dist_node_metadata(
    metadata jsonb NOT NULL
);
ALTER TABLE citus.pg_dist_node_metadata SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_node_metadata TO public;

CREATE FUNCTION pg_catalog.citus_server_id()
    RETURNS uuid
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_server_id$$;
COMMENT ON FUNCTION citus_server_id()
    IS 'generates a random UUID to be used as server identifier';

-- Insert the latest extension version into pg_dist_node_metadata
-- for new installations.
--
-- While users could technically upgrade to an intermediate version
-- everything in Citus fails until it is upgraded to the latest version,
-- so it seems safe to use the latest.
INSERT INTO pg_dist_node_metadata
SELECT jsonb_build_object('server_id', citus_server_id()::text,
                          'last_upgrade_version', default_version)
FROM pg_available_extensions
WHERE name = 'citus';

-- rebalancer functions
CREATE TYPE citus.shard_transfer_mode AS ENUM (
   'auto',
   'force_logical',
   'block_writes'
);

CREATE OR REPLACE FUNCTION master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$master_move_shard_placement$$;

COMMENT ON FUNCTION master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a shard from a the source node to the destination node';

CREATE FUNCTION master_copy_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	do_repair bool DEFAULT true,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$master_copy_shard_placement$$;

COMMENT ON FUNCTION master_copy_shard_placement(shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	do_repair bool,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'copy a shard from the source node to the destination node';

-- intermediate result functions
CREATE OR REPLACE FUNCTION pg_catalog.create_intermediate_result(result_id text, query text)
    RETURNS bigint
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$create_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.create_intermediate_result(text,text)
    IS 'execute a query and write its results to local result file';

CREATE OR REPLACE FUNCTION pg_catalog.broadcast_intermediate_result(result_id text, query text)
    RETURNS bigint
    LANGUAGE C STRICT VOLATILE
    AS 'MODULE_PATHNAME', $$broadcast_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.broadcast_intermediate_result(text,text)
    IS 'execute a query and write its results to an result file on all workers';

CREATE TYPE pg_catalog.citus_copy_format AS ENUM ('csv', 'binary', 'text');

CREATE OR REPLACE FUNCTION pg_catalog.read_intermediate_result(result_id text, format pg_catalog.citus_copy_format default 'csv')
    RETURNS SETOF record
    LANGUAGE C STRICT VOLATILE PARALLEL SAFE
    AS 'MODULE_PATHNAME', $$read_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.read_intermediate_result(text,pg_catalog.citus_copy_format)
    IS 'read a file and return it as a set of records';

CREATE FUNCTION pg_catalog.citus_text_send_as_jsonb(text)
RETURNS bytea
LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $$citus_text_send_as_jsonb$$;

-- Citus json aggregate helpers

CREATE FUNCTION pg_catalog.citus_jsonb_concatenate(state jsonb, val jsonb)
	RETURNS jsonb
	LANGUAGE SQL
AS $function$
	SELECT CASE
		WHEN val IS NULL THEN state
		WHEN jsonb_typeof(state) = 'null' THEN val
		ELSE state || val
	END;
$function$;

CREATE FUNCTION pg_catalog.citus_jsonb_concatenate_final(state jsonb)
	RETURNS jsonb
	LANGUAGE SQL
AS $function$
	SELECT CASE WHEN jsonb_typeof(state) = 'null' THEN NULL ELSE state END;
$function$;

CREATE FUNCTION pg_catalog.citus_json_concatenate(state json, val json)
	RETURNS json
	LANGUAGE SQL
AS $function$
	SELECT CASE
		WHEN val IS NULL THEN state
		WHEN json_typeof(state) = 'null' THEN val
		WHEN json_typeof(state) = 'object' THEN
 			(SELECT json_object_agg(key, value) FROM (
		 		SELECT * FROM json_each(state)
		 		UNION ALL
		 		SELECT * FROM json_each(val)
	 		) t)
		ELSE
	 		(SELECT json_agg(a) FROM (
	 			SELECT json_array_elements(state) AS a
		 		UNION ALL
		 		SELECT json_array_elements(val) AS a
	 		) t)
	END;
$function$;

CREATE FUNCTION pg_catalog.citus_json_concatenate_final(state json)
	RETURNS json
	LANGUAGE SQL
AS $function$
	SELECT CASE WHEN json_typeof(state) = 'null' THEN NULL ELSE state END;
$function$;


-- Citus json aggregates

CREATE AGGREGATE pg_catalog.jsonb_cat_agg(jsonb) (
    SFUNC = citus_jsonb_concatenate,
    FINALFUNC = citus_jsonb_concatenate_final,
    STYPE = jsonb,
    INITCOND = 'null'
);
COMMENT ON AGGREGATE pg_catalog.jsonb_cat_agg(jsonb)
    IS 'concatenate input jsonbs into a single jsonb';

CREATE AGGREGATE pg_catalog.json_cat_agg(json) (
    SFUNC = citus_json_concatenate,
    FINALFUNC = citus_json_concatenate_final,
    STYPE = json,
    INITCOND = 'null'
);
COMMENT ON AGGREGATE pg_catalog.json_cat_agg(json)
    IS 'concatenate input jsons into a single json';

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

CREATE EVENT TRIGGER citus_cascade_to_partition
    ON SQL_DROP
    EXECUTE PROCEDURE citus_drop_trigger();

-- pg_dist_authinfo
CREATE FUNCTION pg_catalog.role_exists(name)
    RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$role_exists$$;
COMMENT ON FUNCTION role_exists(name) IS 'returns whether a role exists';

CREATE FUNCTION pg_catalog.authinfo_valid(text)
	RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$authinfo_valid$$;
COMMENT ON FUNCTION authinfo_valid(text) IS 'returns whether an authinfo is valid';

CREATE TABLE citus.pg_dist_authinfo (
	nodeid integer NOT NULL,
	rolename name NOT NULL
	              CONSTRAINT role_exists
								CHECK (role_exists(rolename)),
	authinfo text NOT NULL
	              CONSTRAINT authinfo_valid
								CHECK (authinfo_valid(authinfo))
);

CREATE UNIQUE INDEX pg_dist_authinfo_identification_index
ON citus.pg_dist_authinfo (rolename, nodeid DESC);

ALTER TABLE citus.pg_dist_authinfo SET SCHEMA pg_catalog;

REVOKE ALL ON pg_catalog.pg_dist_authinfo FROM PUBLIC;


CREATE FUNCTION master_dist_authinfo_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'citus', $$master_dist_authinfo_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_authinfo_cache_invalidate()
    IS 'register authinfo cache invalidation on any modifications';

CREATE FUNCTION task_tracker_conninfo_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'citus', $$task_tracker_conninfo_cache_invalidate$$;
COMMENT ON FUNCTION task_tracker_conninfo_cache_invalidate()
    IS 'invalidate task-tracker conninfo cache';

CREATE TRIGGER dist_authinfo_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_authinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE master_dist_authinfo_cache_invalidate();

CREATE TRIGGER dist_authinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_authinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

-- citus_stat_statements
CREATE FUNCTION pg_catalog.citus_query_stats(OUT queryid bigint,
											 OUT userid oid,
											 OUT dbid oid,
											 OUT executor bigint,
											 OUT partition_key text,
											 OUT calls bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_query_stats$$;

CREATE FUNCTION pg_catalog.citus_stat_statements_reset()
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_stat_statements_reset$$;

CREATE FUNCTION pg_catalog.citus_stat_statements(OUT queryid bigint,
												 OUT userid oid,
												 OUT dbid oid,
												 OUT query text,
												 OUT executor bigint,
												 OUT partition_key text,
												 OUT calls bigint)
RETURNS SETOF record
LANGUAGE plpgsql
AS $citus_stat_statements$
BEGIN
 IF EXISTS (
 	SELECT extname FROM pg_extension
 	WHERE extname = 'pg_stat_statements')
 THEN
 	RETURN QUERY SELECT pss.queryid, pss.userid, pss.dbid, pss.query, cqs.executor,
 						cqs.partition_key, cqs.calls
 				 FROM pg_stat_statements(true) pss
 				 	JOIN citus_query_stats() cqs
 				 	USING (queryid);
 ELSE
    RAISE EXCEPTION 'pg_stat_statements is not installed'
    	USING HINT = 'install pg_stat_statements extension and try again';
 END IF;
END;
$citus_stat_statements$;

CREATE VIEW citus.citus_stat_statements as SELECT * FROM pg_catalog.citus_stat_statements();
ALTER VIEW citus.citus_stat_statements SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stat_statements TO public;

CREATE FUNCTION pg_catalog.citus_executor_name(executor_type int)
RETURNS TEXT
LANGUAGE plpgsql
AS $function$
BEGIN
	IF (executor_type = 1) THEN
		RETURN 'real-time';
	ELSIF (executor_type = 2) THEN
		RETURN 'task-tracker';
	ELSIF (executor_type = 3) THEN
		RETURN 'router';
	ELSIF (executor_type = 4) THEN
		RETURN 'insert-select';
	ELSE
		RETURN 'unknown';
	END IF;
END;
$function$;

DROP VIEW pg_catalog.citus_stat_statements;

CREATE VIEW citus.citus_stat_statements AS
SELECT
  queryid,
  userid,
  dbid,
  query,
  pg_catalog.citus_executor_name(executor::int) AS executor,
  partition_key,
  calls
FROM pg_catalog.citus_stat_statements();
ALTER VIEW citus.citus_stat_statements SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stat_statements TO public;

REVOKE ALL ON FUNCTION pg_catalog.citus_stat_statements_reset() FROM PUBLIC;

-- pg_dist_poolinfo
CREATE FUNCTION pg_catalog.poolinfo_valid(text)
	RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$poolinfo_valid$$;
COMMENT ON FUNCTION pg_catalog.poolinfo_valid(text) IS 'returns whether a poolinfo is valid';

CREATE TABLE citus.pg_dist_poolinfo (
    nodeid integer PRIMARY KEY
                   REFERENCES pg_dist_node(nodeid)
                              ON DELETE CASCADE,
    poolinfo text NOT NULL
	              CONSTRAINT poolinfo_valid
								CHECK (poolinfo_valid(poolinfo))
);

ALTER TABLE citus.pg_dist_poolinfo SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_poolinfo TO public;

ALTER FUNCTION master_dist_authinfo_cache_invalidate()
RENAME TO master_conninfo_cache_invalidate;

CREATE TRIGGER dist_poolinfo_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_poolinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE master_conninfo_cache_invalidate();

CREATE TRIGGER dist_poolinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_poolinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

RESET search_path;
