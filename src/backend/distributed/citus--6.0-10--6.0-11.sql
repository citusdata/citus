SET search_path = 'pg_catalog';

CREATE SEQUENCE citus.pg_dist_colocationid_seq
	MINVALUE 1
	MAXVALUE 4294967296;

ALTER SEQUENCE citus.pg_dist_colocationid_seq SET SCHEMA pg_catalog;

/* add pg_dist_colocation */
CREATE TABLE citus.pg_dist_colocation(
	colocationid int NOT NULL PRIMARY KEY,
	shardcount int NOT NULL,
	replicationfactor int NOT NULL,
	distributioncolumntype oid NOT NULL
);

ALTER TABLE citus.pg_dist_colocation SET SCHEMA pg_catalog;

CREATE INDEX pg_dist_colocation_configuration_index
ON pg_dist_colocation USING btree(shardcount, replicationfactor, distributioncolumntype);

CREATE FUNCTION create_distributed_table(table_name regclass,
										 distribution_column text,
										 distribution_type citus.distribution_type DEFAULT 'hash')
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$create_distributed_table$$;
COMMENT ON FUNCTION create_distributed_table(table_name regclass,
											 distribution_column text,
											 distribution_type citus.distribution_type)
    IS 'creates a distributed table';


CREATE OR REPLACE FUNCTION pg_catalog.citus_drop_trigger()
    RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $cdbdt$
DECLARE
    v_obj record;
    sequence_names text[] := '{}';
    node_names text[] := '{}';
    node_ports bigint[] := '{}';
    node_name text;
    node_port bigint;
    table_colocation_id integer;
BEGIN
    -- collect set of dropped sequences to drop on workers later
    SELECT array_agg(object_identity) INTO sequence_names
    FROM pg_event_trigger_dropped_objects()
    WHERE object_type = 'sequence';

    -- Must accumulate set of affected nodes before deleting placements, as
    -- master_drop_all_shards will erase their rows, making it impossible for
    -- us to know where to drop sequences (which must be dropped after shards,
    -- since they have default value expressions which depend on sequences).
    SELECT array_agg(sp.nodename), array_agg(sp.nodeport)
    INTO node_names, node_ports
    FROM pg_event_trigger_dropped_objects() AS dobj,
         pg_dist_shard AS s,
         pg_dist_shard_placement AS sp
    WHERE dobj.object_type IN ('table', 'foreign table')
      AND dobj.objid = s.logicalrelid
      AND s.shardid = sp.shardid;

    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF v_obj.object_type NOT IN ('table', 'foreign table') THEN
           CONTINUE;
        END IF;

        -- nothing to do if not a distributed table
        IF NOT EXISTS(SELECT * FROM pg_dist_partition WHERE logicalrelid = v_obj.objid) THEN
            CONTINUE;
        END IF;

        -- ensure all shards are dropped
        PERFORM master_drop_all_shards(v_obj.objid, v_obj.schema_name, v_obj.object_name);

        SELECT colocationid INTO table_colocation_id FROM pg_dist_partition WHERE logicalrelid = v_obj.objid;

        -- delete partition entry
        DELETE FROM pg_dist_partition WHERE logicalrelid = v_obj.objid;

        IF NOT EXISTS(SELECT * FROM pg_dist_partition WHERE colocationId = table_colocation_id) THEN
            DELETE FROM pg_dist_colocation WHERE colocationId = table_colocation_id;
        END IF;
    END LOOP;

    IF cardinality(sequence_names) = 0 THEN
        RETURN;
    END IF;

    FOR node_name, node_port IN
    SELECT DISTINCT name, port
    FROM unnest(node_names, node_ports) AS nodes(name, port)
    LOOP
        PERFORM master_drop_sequences(sequence_names, node_name, node_port);
    END LOOP;
END;
$cdbdt$;

COMMENT ON FUNCTION citus_drop_trigger()
    IS 'perform checks and actions at the end of DROP actions';

RESET search_path;
