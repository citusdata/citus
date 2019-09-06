/* citus--8.3-1--8.4-1 */

/* bump version to 8.4-1 */
CREATE SCHEMA IF NOT EXISTS citus_internal;

-- move citus internal functions to citus_internal to make space in the citus schema for
-- our public interface
ALTER FUNCTION citus.find_groupid_for_node SET SCHEMA citus_internal;
ALTER FUNCTION citus.pg_dist_node_trigger_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.pg_dist_shard_placement_trigger_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.refresh_isolation_tester_prepared_statement SET SCHEMA citus_internal;
ALTER FUNCTION citus.replace_isolation_tester_func SET SCHEMA citus_internal;
ALTER FUNCTION citus.restore_isolation_tester_func SET SCHEMA citus_internal;

CREATE OR REPLACE FUNCTION citus_internal.pg_dist_shard_placement_trigger_func()
RETURNS TRIGGER AS $$
  BEGIN
    IF (TG_OP = 'DELETE') THEN
      DELETE FROM pg_dist_placement WHERE placementid = OLD.placementid;
      RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
      UPDATE pg_dist_placement
        SET shardid = NEW.shardid, shardstate = NEW.shardstate,
            shardlength = NEW.shardlength, placementid = NEW.placementid,
            groupid = citus_internal.find_groupid_for_node(NEW.nodename, NEW.nodeport)
        WHERE placementid = OLD.placementid;
      RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
      INSERT INTO pg_dist_placement
        (placementid, shardid, shardstate, shardlength, groupid)
      VALUES (NEW.placementid, NEW.shardid, NEW.shardstate, NEW.shardlength,
        citus_internal.find_groupid_for_node(NEW.nodename, NEW.nodeport));
      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_catalog.master_unmark_object_distributed(classid oid, objid oid, objsubid int)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_unmark_object_distributed$$;
COMMENT ON FUNCTION pg_catalog.master_unmark_object_distributed(classid oid, objid oid, objsubid int)
    IS 'remove an object address from citus.pg_dist_object once the object has been deleted';

CREATE TABLE citus.pg_dist_object (
    classid oid NOT NULL,
    objid oid NOT NULL,
    objsubid integer NOT NULL,

    -- fields used for upgrades
    type text DEFAULT NULL,
    object_names text[] DEFAULT NULL,
    object_args text[] DEFAULT NULL,

    CONSTRAINT pg_dist_object_pkey PRIMARY KEY (classid, objid, objsubid)
);

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
        -- first drop the table and metadata on the workers
        -- then drop all the shards on the workers
        -- finally remove the pg_dist_partition entry on the coordinator
        PERFORM master_remove_distributed_table_metadata_from_workers(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);
        PERFORM master_remove_partition_metadata(v_obj.objid, v_obj.schema_name, v_obj.object_name);
    END LOOP;

    IF cardinality(sequence_names) > 0 THEN
        PERFORM master_drop_sequences(sequence_names);
    END IF;

    -- remove entries from citus.pg_dist_object for all dropped root (objsubid = 0) objects
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        PERFORM master_unmark_object_distributed(v_obj.classid, v_obj.objid, v_obj.objsubid);
    END LOOP;
END;
$cdbdt$;
COMMENT ON FUNCTION pg_catalog.citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';


CREATE OR REPLACE FUNCTION pg_catalog.citus_prepare_pg_upgrade()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cppu$
BEGIN
    --
    -- backup citus catalog tables
    --
    CREATE TABLE public.pg_dist_partition AS SELECT * FROM pg_catalog.pg_dist_partition;
    CREATE TABLE public.pg_dist_shard AS SELECT * FROM pg_catalog.pg_dist_shard;
    CREATE TABLE public.pg_dist_placement AS SELECT * FROM pg_catalog.pg_dist_placement;
    CREATE TABLE public.pg_dist_node_metadata AS SELECT * FROM pg_catalog.pg_dist_node_metadata;
    CREATE TABLE public.pg_dist_node AS SELECT * FROM pg_catalog.pg_dist_node;
    CREATE TABLE public.pg_dist_local_group AS SELECT * FROM pg_catalog.pg_dist_local_group;
    CREATE TABLE public.pg_dist_transaction AS SELECT * FROM pg_catalog.pg_dist_transaction;
    CREATE TABLE public.pg_dist_colocation AS SELECT * FROM pg_catalog.pg_dist_colocation;
    -- enterprise catalog tables
    CREATE TABLE public.pg_dist_authinfo AS SELECT * FROM pg_catalog.pg_dist_authinfo;
    CREATE TABLE public.pg_dist_poolinfo AS SELECT * FROM pg_catalog.pg_dist_poolinfo;

    -- store upgrade stable identifiers on pg_dist_object catalog
    UPDATE citus.pg_dist_object
       SET (type, object_names, object_args) = (SELECT * FROM pg_identify_object_as_address(classid, objid, objsubid));
END;
$cppu$;

COMMENT ON FUNCTION pg_catalog.citus_prepare_pg_upgrade()
    IS 'perform tasks to copy citus settings to a location that could later be restored after pg_upgrade is done';


CREATE OR REPLACE FUNCTION pg_catalog.citus_finish_pg_upgrade()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cppu$
DECLARE
    table_name regclass;
    command text;
    trigger_name text;
BEGIN
    --
    -- restore citus catalog tables
    --
    INSERT INTO pg_catalog.pg_dist_partition SELECT * FROM public.pg_dist_partition;
    INSERT INTO pg_catalog.pg_dist_shard SELECT * FROM public.pg_dist_shard;
    INSERT INTO pg_catalog.pg_dist_placement SELECT * FROM public.pg_dist_placement;
    INSERT INTO pg_catalog.pg_dist_node_metadata SELECT * FROM public.pg_dist_node_metadata;
    INSERT INTO pg_catalog.pg_dist_node SELECT * FROM public.pg_dist_node;
    INSERT INTO pg_catalog.pg_dist_local_group SELECT * FROM public.pg_dist_local_group;
    INSERT INTO pg_catalog.pg_dist_transaction SELECT * FROM public.pg_dist_transaction;
    INSERT INTO pg_catalog.pg_dist_colocation SELECT * FROM public.pg_dist_colocation;
    -- enterprise catalog tables
    INSERT INTO pg_catalog.pg_dist_authinfo SELECT * FROM public.pg_dist_authinfo;
    INSERT INTO pg_catalog.pg_dist_poolinfo SELECT * FROM public.pg_dist_poolinfo;

    --
    -- drop backup tables
    --
    DROP TABLE public.pg_dist_authinfo;
    DROP TABLE public.pg_dist_colocation;
    DROP TABLE public.pg_dist_local_group;
    DROP TABLE public.pg_dist_node;
    DROP TABLE public.pg_dist_node_metadata;
    DROP TABLE public.pg_dist_partition;
    DROP TABLE public.pg_dist_placement;
    DROP TABLE public.pg_dist_poolinfo;
    DROP TABLE public.pg_dist_shard;
    DROP TABLE public.pg_dist_transaction;

    --
    -- reset sequences
    --
    PERFORM setval('pg_catalog.pg_dist_shardid_seq', (SELECT MAX(shardid)+1 AS max_shard_id FROM pg_dist_shard), false);
    PERFORM setval('pg_catalog.pg_dist_placement_placementid_seq', (SELECT MAX(placementid)+1 AS max_placement_id FROM pg_dist_placement), false);
    PERFORM setval('pg_catalog.pg_dist_groupid_seq', (SELECT MAX(groupid)+1 AS max_group_id FROM pg_dist_node), false);
    PERFORM setval('pg_catalog.pg_dist_node_nodeid_seq', (SELECT MAX(nodeid)+1 AS max_node_id FROM pg_dist_node), false);
    PERFORM setval('pg_catalog.pg_dist_colocationid_seq', (SELECT MAX(colocationid)+1 AS max_colocation_id FROM pg_dist_colocation), false);

    --
    -- register triggers
    --
    FOR table_name IN SELECT logicalrelid FROM pg_catalog.pg_dist_partition
    LOOP
        trigger_name := 'truncate_trigger_' || table_name::oid;
        command := 'create trigger ' || trigger_name || ' after truncate on ' || table_name || ' execute procedure pg_catalog.citus_truncate_trigger()';
        EXECUTE command;
        command := 'update pg_trigger set tgisinternal = true where tgname = ' || quote_literal(trigger_name);
        EXECUTE command;
    END LOOP;

    --
    -- set dependencies
    --
    INSERT INTO pg_depend
    SELECT
        'pg_class'::regclass::oid as classid,
        p.logicalrelid::regclass::oid as objid,
        0 as objsubid,
        'pg_extension'::regclass::oid as refclassid,
        (select oid from pg_extension where extname = 'citus') as refobjid,
        0 as refobjsubid ,
        'n' as deptype
    FROM pg_catalog.pg_dist_partition p;

    -- restore pg_dist_object from the stable identifiers
    WITH old_records AS (
        DELETE FROM
            citus.pg_dist_object
        RETURNING
            type,
            object_names,
            object_args
    )
    INSERT INTO citus.pg_dist_object (classid, objid, objsubid)
    SELECT
        address.classid,
        address.objid,
        address.objsubid
    FROM
        old_records naming,
        pg_get_object_address(naming.type, naming.object_names, naming.object_args) address;
END;
$cppu$;

COMMENT ON FUNCTION pg_catalog.citus_finish_pg_upgrade()
    IS 'perform tasks to restore citus settings from a location that has been prepared before pg_upgrade';

/*
 * We truncate pg_dist_node during metadata syncing, but we do not want
 * this to cascade to pg_dist_poolinfo, which is generally maintained
 * by the operator.
 */
ALTER TABLE pg_dist_poolinfo DROP CONSTRAINT pg_dist_poolinfo_nodeid_fkey;

SET search_path = 'pg_catalog';

DROP EXTENSION IF EXISTS shard_rebalancer;

-- get_rebalance_table_shards_plan shows the actual events that will be performed
-- if a rebalance operation will be performed with the same arguments, which allows users
-- to understand the impact of the change overall availability of the application and
-- network trafic.
--
CREATE OR REPLACE FUNCTION get_rebalance_table_shards_plan(relation regclass,
                                                           threshold float4 default 0.1,
                                                           max_shard_moves int default 1000000,
                                                           excluded_shard_list bigint[] default '{}')
    RETURNS TABLE (table_name regclass,
                   shardid bigint,
                   shard_size bigint,
                   sourcename text,
                   sourceport int,
                   targetname text,
                   targetport int)
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION get_rebalance_table_shards_plan(regclass, float4, int, bigint[])
IS 'returns the list of shard placement moves to be done on a rebalance operation';

-- get_rebalance_progress returns the list of shard placement move operations along with
-- their progressions for ongoing rebalance operations.
--
CREATE OR REPLACE FUNCTION get_rebalance_progress()
  RETURNS TABLE(sessionid integer,
                table_name regclass,
                shardid bigint,
                shard_size bigint,
                sourcename text,
                sourceport int,
                targetname text,
                targetport int,
                progress bigint)
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;
COMMENT ON FUNCTION get_rebalance_progress()
    IS 'provides progress information about the ongoing rebalance operations';


-- replicate_table_shards uses the shard rebalancer's C UDF functions to replicate
-- under-replicated shards of the given table.
--
CREATE FUNCTION replicate_table_shards(relation regclass,
                                       shard_replication_factor int default current_setting('citus.shard_replication_factor')::int,
                                       max_shard_copies int default 1000000,
                                       excluded_shard_list bigint[] default '{}',
                                       shard_transfer_mode citus.shard_transfer_mode default 'auto')
  RETURNS VOID
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;
COMMENT ON FUNCTION replicate_table_shards(regclass, int, int, bigint[], citus.shard_transfer_mode)
    IS 'replicates under replicated shards of the the given table';

-- rebalance_table_shards uses the shard rebalancer's C UDF functions to rebalance
-- shards of the given relation.
--
CREATE OR REPLACE FUNCTION rebalance_table_shards(relation regclass,
                                                  threshold float4 default 0,
                                                  max_shard_moves int default 1000000,
                                                  excluded_shard_list bigint[] default '{}',
                                                  shard_transfer_mode citus.shard_transfer_mode default 'auto')
  RETURNS VOID
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION rebalance_table_shards(regclass, float4, int, bigint[], citus.shard_transfer_mode)
    IS 'rebalance the shards of the given table across the worker nodes (including colocated shards of other tables)';

RESET search_path;
