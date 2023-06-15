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


    IF substring(current_Setting('server_version'), '\d+')::int >= 14 THEN
    EXECUTE $cmd$
        -- disable propagation to prevent EnsureCoordinator errors
        -- the aggregate created here does not depend on Citus extension (yet)
        -- since we add the dependency with the next command
        SET citus.enable_ddl_propagation TO OFF;
        CREATE AGGREGATE array_cat_agg(anycompatiblearray) (SFUNC = array_cat, STYPE = anycompatiblearray);
        COMMENT ON AGGREGATE array_cat_agg(anycompatiblearray)
        IS 'concatenate input arrays into a single array';
        RESET citus.enable_ddl_propagation;
    $cmd$;
    ELSE
    EXECUTE $cmd$
        SET citus.enable_ddl_propagation TO OFF;
        CREATE AGGREGATE array_cat_agg(anyarray) (SFUNC = array_cat, STYPE = anyarray);
        COMMENT ON AGGREGATE array_cat_agg(anyarray)
        IS 'concatenate input arrays into a single array';
        RESET citus.enable_ddl_propagation;
    $cmd$;
    END IF;

    --
    -- Citus creates the array_cat_agg but because of a compatibility
    -- issue between pg13-pg14, we drop and create it during upgrade.
    -- And as Citus creates it, there needs to be a dependency to the
    -- Citus extension, so we create that dependency here.
    -- We are not using:
    --  ALTER EXENSION citus DROP/CREATE AGGREGATE array_cat_agg
    -- because we don't have an easy way to check if the aggregate
    -- exists with anyarray type or anycompatiblearray type.

    INSERT INTO pg_depend
    SELECT
        'pg_proc'::regclass::oid as classid,
        (SELECT oid FROM pg_proc WHERE proname = 'array_cat_agg') as objid,
        0 as objsubid,
        'pg_extension'::regclass::oid as refclassid,
        (select oid from pg_extension where extname = 'citus') as refobjid,
        0 as refobjsubid ,
        'e' as deptype;

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
    INSERT INTO pg_catalog.pg_dist_cleanup SELECT * FROM public.pg_dist_cleanup;
    INSERT INTO pg_catalog.pg_dist_schema SELECT schemaname::regnamespace, colocationid FROM public.pg_dist_schema;
    -- enterprise catalog tables
    INSERT INTO pg_catalog.pg_dist_authinfo SELECT * FROM public.pg_dist_authinfo;
    INSERT INTO pg_catalog.pg_dist_poolinfo SELECT * FROM public.pg_dist_poolinfo;

    INSERT INTO pg_catalog.pg_dist_rebalance_strategy SELECT
        name,
        default_strategy,
        shard_cost_function::regprocedure::regproc,
        node_capacity_function::regprocedure::regproc,
        shard_allowed_on_node_function::regprocedure::regproc,
        default_threshold,
        minimum_threshold,
        improvement_threshold
    FROM public.pg_dist_rebalance_strategy;

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
    DROP TABLE public.pg_dist_rebalance_strategy;
    DROP TABLE public.pg_dist_cleanup;
    DROP TABLE public.pg_dist_schema;
    --
    -- reset sequences
    --
    PERFORM setval('pg_catalog.pg_dist_shardid_seq', (SELECT MAX(shardid)+1 AS max_shard_id FROM pg_dist_shard), false);
    PERFORM setval('pg_catalog.pg_dist_placement_placementid_seq', (SELECT MAX(placementid)+1 AS max_placement_id FROM pg_dist_placement), false);
    PERFORM setval('pg_catalog.pg_dist_groupid_seq', (SELECT MAX(groupid)+1 AS max_group_id FROM pg_dist_node), false);
    PERFORM setval('pg_catalog.pg_dist_node_nodeid_seq', (SELECT MAX(nodeid)+1 AS max_node_id FROM pg_dist_node), false);
    PERFORM setval('pg_catalog.pg_dist_colocationid_seq', (SELECT MAX(colocationid)+1 AS max_colocation_id FROM pg_dist_colocation), false);
    PERFORM setval('pg_catalog.pg_dist_operationid_seq', (SELECT MAX(operation_id)+1 AS max_operation_id FROM pg_dist_cleanup), false);
    PERFORM setval('pg_catalog.pg_dist_cleanup_recordid_seq', (SELECT MAX(record_id)+1 AS max_record_id FROM pg_dist_cleanup), false);
    PERFORM setval('pg_catalog.pg_dist_clock_logical_seq', (SELECT last_value FROM public.pg_dist_clock_logical_seq), false);
    DROP TABLE public.pg_dist_clock_logical_seq;



    --
    -- register triggers
    --
    FOR table_name IN SELECT logicalrelid FROM pg_catalog.pg_dist_partition JOIN pg_class ON (logicalrelid = oid) WHERE relkind <> 'f'
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

    -- set dependencies for columnar table access method
    PERFORM columnar_internal.columnar_ensure_am_depends_catalog();

    -- restore pg_dist_object from the stable identifiers
    TRUNCATE pg_catalog.pg_dist_object;
    INSERT INTO pg_catalog.pg_dist_object (classid, objid, objsubid, distribution_argument_index, colocationid)
    SELECT
        address.classid,
        address.objid,
        address.objsubid,
        naming.distribution_argument_index,
        naming.colocationid
    FROM
        public.pg_dist_object naming,
        pg_catalog.pg_get_object_address(naming.type, naming.object_names, naming.object_args) address;

    DROP TABLE public.pg_dist_object;
END;
$cppu$;

COMMENT ON FUNCTION pg_catalog.citus_finish_pg_upgrade()
    IS 'perform tasks to restore citus settings from a location that has been prepared before pg_upgrade';
