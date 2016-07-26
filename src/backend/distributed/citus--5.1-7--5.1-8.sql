CREATE FUNCTION pg_catalog.master_drop_sequences(sequence_names text[],
												 node_name text,
												 node_port bigint)
	RETURNS bool
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_drop_sequences$$;
COMMENT ON FUNCTION pg_catalog.master_drop_sequences(text[], text, bigint)
	IS 'drop specified sequences from a node';

REVOKE ALL ON FUNCTION pg_catalog.master_drop_sequences(text[], text, bigint) FROM PUBLIC;

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

		-- delete partition entry
		DELETE FROM pg_dist_partition WHERE logicalrelid = v_obj.objid;
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
