-- run_command_on_coordinator tries to closely follow the semantics of run_command_on_all_nodes,
-- but only runs the command on the coordinator
CREATE FUNCTION pg_catalog.run_command_on_coordinator(command text, give_warning_for_connection_errors bool default false,
													  OUT nodeid int, OUT success bool, OUT result text)
	RETURNS SETOF record
	LANGUAGE plpgsql
	AS $function$
DECLARE
	nodenames text[];
	ports int[];
	commands text[];
	coordinator_is_in_metadata boolean;
	parallel boolean := false;
BEGIN
	WITH citus_nodes AS (
		SELECT * FROM pg_dist_node
		WHERE isactive AND nodecluster = current_setting('citus.cluster_name') AND groupid = 0
		AND (
			(current_setting('citus.use_secondary_nodes') = 'never' AND noderole = 'primary')
			OR
			(current_setting('citus.use_secondary_nodes') = 'always' AND noderole = 'secondary')
		)
		ORDER BY nodename, nodeport
	)
	SELECT array_agg(citus_nodes.nodename), array_agg(citus_nodes.nodeport), array_agg(command), count(*) > 0
	FROM citus_nodes
	INTO nodenames, ports, commands, coordinator_is_in_metadata;

	IF NOT coordinator_is_in_metadata THEN
		-- This will happen when we call this function on coordinator and
		-- the coordinator is not added to the metadata.
		-- We'll manually add current node to the lists to actually run on all nodes.
		-- But when the coordinator is not added to metadata and this function
		-- is called from a worker node, this will not be enough and we'll
		-- not be able run on all nodes.
		IF citus_is_coordinator() THEN
			SELECT
				array_append(nodenames, current_setting('citus.local_hostname')),
				array_append(ports, current_setting('port')::int),
				array_append(commands, command)
			INTO nodenames, ports, commands;
		ELSE
			RAISE EXCEPTION 'the coordinator is not added to the metadata'
			USING HINT = 'Add the node as a coordinator by using: SELECT citus_set_coordinator_host(''<hostname>'')';
		END IF;
	END IF;

	FOR nodeid, success, result IN
		SELECT coalesce(pg_dist_node.nodeid, 0) AS nodeid, mrow.success, mrow.result
		FROM master_run_on_worker(nodenames, ports, commands, parallel) mrow
		LEFT JOIN pg_dist_node ON mrow.node_name = pg_dist_node.nodename AND mrow.node_port = pg_dist_node.nodeport
	LOOP
		IF give_warning_for_connection_errors AND NOT success THEN
			RAISE WARNING 'Error on node with node id %: %', nodeid, result;
		END IF;
		RETURN NEXT;
	END LOOP;
END;
$function$;
