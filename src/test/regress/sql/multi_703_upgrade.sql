ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 103000;

-- tests that the upgrade from 7.0-2 to 7.0-3 properly migrates shard placements

DROP EXTENSION citus;
SET citus.enable_version_checks TO 'false';

CREATE EXTENSION citus VERSION '7.0-2';

INSERT INTO pg_dist_shard_placement
  (placementid, shardid, shardstate, shardlength, nodename, nodeport) VALUES
  (1, 1, 1, 0, 'localhost', :worker_1_port);

-- if there are no worker nodes which match the shards this should fail
ALTER EXTENSION citus UPDATE TO '7.0-3';

-- if you add a matching worker the upgrade should succeed
SELECT master_add_node('localhost', :worker_1_port);
ALTER EXTENSION citus UPDATE TO '7.0-3';

SELECT * FROM pg_dist_placement;

-- reset and prepare for the rest of the tests
DROP EXTENSION citus;
CREATE EXTENSION citus;
