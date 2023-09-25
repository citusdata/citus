CREATE SCHEMA citus_schema_move;
SET search_path TO citus_schema_move;

SET citus.next_shard_id TO 2220000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

SELECT master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

-- Due to a race condition that happens in TransferShards() when the same shard id
-- is used to create the same shard on a different worker node, need to call
-- citus_cleanup_orphaned_resources() to clean up any orphaned resources before
-- running the tests.
--
-- See https://github.com/citusdata/citus/pull/7180#issuecomment-1706786615.

CALL citus_cleanup_orphaned_resources();

SET client_min_messages TO NOTICE;

-- test null input, should be no-op
SELECT citus_schema_move(schema_id=>null, target_node_name=>null, target_node_port=>null, shard_transfer_mode=>null);
SELECT citus_schema_move(schema_id=>null, target_node_id=>null, shard_transfer_mode=>null);
SELECT citus_schema_move(schema_id=>null, target_node_id=>null, shard_transfer_mode=>null);

SET citus.enable_schema_based_sharding TO ON;

CREATE SCHEMA s1;

-- test invalid schema
SELECT citus_schema_move('no_such_schema', 'dummy_node_name', 1234);
SELECT citus_schema_move('no_such_schema', 1234);

-- test regular schema
SELECT citus_schema_move('citus_schema_move', 'dummy_node_name', 1234);
SELECT citus_schema_move('citus_schema_move', 1234);

-- test empty distributed schema
SELECT citus_schema_move('s1', 'dummy_node_name', 1234);
SELECT citus_schema_move('s1', 1234);

CREATE TABLE s1.t1 (a int);

-- test invalid node name / port / id
SELECT citus_schema_move('s1', 'dummy_node_name', 1234);
SELECT citus_schema_move('s1', 1234);

-- errors due to missing pkey / replicate ident.
SELECT citus_schema_move('s1', nodename, nodeport) FROM pg_dist_node
WHERE isactive AND shouldhaveshards AND noderole='primary' AND
     (nodename, nodeport) NOT IN (
        SELECT nodename, nodeport FROM citus_shards WHERE table_name = 's1.t1'::regclass
        );

-- errors as we try to move the schema to the same node
SELECT citus_schema_move('s1', nodename, nodeport, 'block_writes')
FROM citus_shards
JOIN pg_dist_node USING (nodename, nodeport)
WHERE noderole = 'primary' AND table_name = 's1.t1'::regclass;

SELECT citus_schema_move('s1', nodeid, 'block_writes')
FROM citus_shards
JOIN pg_dist_node USING (nodename, nodeport)
WHERE noderole = 'primary' AND table_name = 's1.t1'::regclass;

-- returns id, host name and host port of a non-coordinator node that given schema can be moved to
CREATE OR REPLACE FUNCTION get_non_coord_candidate_node_for_schema_move(
    schema_id regnamespace)
RETURNS TABLE (nodeid integer, nodename text, nodeport integer)
SET search_path TO 'pg_catalog, public'
AS $func$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_dist_schema WHERE schemaid = schema_id)
    THEN
        RAISE EXCEPTION '% is not a distributed schema', schema_id;
    END IF;

    CREATE TEMP TABLE nodeid_nodename_nodeport ON COMMIT DROP AS
    SELECT pdn1.nodeid, pdn1.nodename, pdn1.nodeport
    FROM pg_dist_node pdn1
    WHERE isactive AND shouldhaveshards AND noderole='primary' AND groupid != 0 AND
          (pdn1.nodename, pdn1.nodeport) NOT IN (
            SELECT cs.nodename, cs.nodeport
            FROM citus_shards cs
            JOIN pg_dist_node pdn2
            ON cs.nodename = pdn2.nodename AND cs.nodeport = pdn2.nodeport
            WHERE pdn2.noderole='primary' AND starts_with(table_name::text, schema_id::text)
            );

    IF NOT EXISTS (SELECT 1 FROM nodeid_nodename_nodeport)
    THEN
        RAISE EXCEPTION 'could not determine a node to move the schema to';
    END IF;

    RETURN QUERY SELECT * FROM nodeid_nodename_nodeport LIMIT 1;
END;
$func$ LANGUAGE plpgsql;

CREATE TABLE s1.t2 (a int);

-- move the schema to a different node

SELECT nodeid AS s1_new_nodeid, quote_literal(nodename) AS s1_new_nodename, nodeport AS s1_new_nodeport
FROM get_non_coord_candidate_node_for_schema_move('s1') \gset

SELECT citus_schema_move('s1', :s1_new_nodename, :s1_new_nodeport, 'block_writes');

SELECT (:s1_new_nodename, :s1_new_nodeport) = ALL(SELECT nodename, nodeport FROM citus_shards JOIN pg_dist_node USING (nodename, nodeport) WHERE noderole = 'primary' AND starts_with(table_name::text, 's1'::text));

SELECT nodeid AS s1_new_nodeid, quote_literal(nodename) AS s1_new_nodename, nodeport AS s1_new_nodeport
FROM get_non_coord_candidate_node_for_schema_move('s1') \gset

SELECT citus_schema_move('s1', :s1_new_nodeid, 'block_writes');

SELECT (:s1_new_nodename, :s1_new_nodeport) = ALL(SELECT nodename, nodeport FROM citus_shards JOIN pg_dist_node USING (nodename, nodeport) WHERE noderole = 'primary' AND starts_with(table_name::text, 's1'::text));

-- move the schema to the coordinator

SELECT citus_schema_move('s1', 'localhost', :master_port, 'block_writes');

SELECT ('localhost', :master_port) = ALL(SELECT nodename, nodeport FROM citus_shards JOIN pg_dist_node USING (nodename, nodeport) WHERE noderole = 'primary' AND starts_with(table_name::text, 's1'::text));

-- move the schema away from the coordinator

SELECT nodeid AS s1_new_nodeid, quote_literal(nodename) AS s1_new_nodename, nodeport AS s1_new_nodeport
FROM get_non_coord_candidate_node_for_schema_move('s1') \gset

SELECT citus_schema_move('s1', :s1_new_nodename, :s1_new_nodeport, 'block_writes');

SELECT (:s1_new_nodename, :s1_new_nodeport) = ALL(SELECT nodename, nodeport FROM citus_shards JOIN pg_dist_node USING (nodename, nodeport) WHERE noderole = 'primary' AND starts_with(table_name::text, 's1'::text));

CREATE USER tenantuser superuser;
SET ROLE tenantuser;

CREATE SCHEMA s2;
CREATE TABLE s2.t1 (a int);
CREATE TABLE s2.t2 (a int);

CREATE USER regularuser;
SET ROLE regularuser;

-- throws an error as the user is not the owner of the schema
SELECT citus_schema_move('s2', 'dummy_node', 1234);

-- assign all tables to regularuser
RESET ROLE;
SELECT result FROM run_command_on_all_nodes($$ REASSIGN OWNED BY tenantuser TO regularuser; $$);

GRANT USAGE ON SCHEMA citus_schema_move TO regularuser;

SET ROLE regularuser;

SELECT nodeid AS s2_new_nodeid, quote_literal(nodename) AS s2_new_nodename, nodeport AS s2_new_nodeport
FROM get_non_coord_candidate_node_for_schema_move('s2') \gset

SELECT citus_schema_move('s2', :s2_new_nodename, :s2_new_nodeport, 'force_logical');

SELECT (:s2_new_nodename, :s2_new_nodeport) = ALL(SELECT nodename, nodeport FROM citus_shards JOIN pg_dist_node USING (nodename, nodeport) WHERE noderole = 'primary' AND starts_with(table_name::text, 's2'::text));

SET client_min_messages TO WARNING;
DROP SCHEMA s2 CASCADE;
SET client_min_messages TO NOTICE;

RESET ROLE;

REVOKE USAGE ON SCHEMA citus_schema_move FROM regularuser;
DROP ROLE regularuser, tenantuser;

RESET citus.enable_schema_based_sharding;

SET client_min_messages TO WARNING;
DROP SCHEMA citus_schema_move, s1 CASCADE;
