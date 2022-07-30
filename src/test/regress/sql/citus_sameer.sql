/*
Citus Shard Split Test.The test is model similar to 'shard_move_constraints'.
Here is a high level overview of test plan:
 1. Create a table 'sensors' (ShardCount = 2) to be split. Add indexes and statistics on this table.
 2. Create two other tables: 'reference_table' and 'colocated_dist_table', co-located with sensors.
 3. Create Foreign key constraints between the two co-located distributed tables.
 4. Load data into the three tables.
 5. Move one of the shards for 'sensors' to test ShardMove -> Split.
 6. Trigger Split on both shards of 'sensors'. This will also split co-located tables.
 7. Move one of the split shard to test Split -> ShardMove.
 8. Split an already split shard second time on a different schema.
*/

CREATE SCHEMA "citus_split_test_schema";

CREATE ROLE test_split_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_split_test_schema" TO test_split_role;
SET ROLE test_split_role;

SET search_path TO "citus_split_test_schema";
SET citus.next_shard_id TO 8981000;
SET citus.next_placement_id TO 8610000;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

-- BEGIN: Create table to split, along with other co-located tables. Add indexes, statistics etc.
CREATE TABLE sensors(
    measureid               integer,
    eventdatetime           date,
    measure_data            jsonb,
	meaure_quantity         decimal(15, 2),
    measure_status          char(1),
	measure_comment         varchar(44),
    PRIMARY KEY (measureid, eventdatetime, measure_data));

SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');
INSERT INTO sensors SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus' FROM generate_series(0,1000)i;
-- END: Create table to split, along with other co-located tables. Add indexes, statistics etc.


-- BEGIN : Move one shard before we split it.
\c - postgres - :master_port
SET ROLE test_split_role;
SET search_path TO "citus_split_test_schema";
SET citus.next_shard_id TO 8981007;
SET citus.defer_drop_after_shard_move TO OFF;

SELECT citus_move_shard_placement(8981000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
-- END : Move one shard before we split it.

-- BEGIN : Set node id variables
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
-- END   : Set node id variables

-- BEGIN : Split two shards : One with move and One without move.
-- Perform 2 way split
SELECT * FROM citus_shards WHERE nodeport IN (:worker_1_port, :worker_2_port);
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981000,
    ARRAY['-1073741824'],
    ARRAY[:worker_2_node, :worker_2_node],
    'force_logical');
SELECT * FROM citus_shards WHERE nodeport IN (:worker_1_port, :worker_2_port);

\c - - - :worker_2_port
SELECT slot_name FROM pg_replication_slots;

\c - - - :master_port
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981001,
    ARRAY['536870911', '1610612735'],
    ARRAY[:worker_2_node, :worker_2_node, :worker_2_node],
    'force_logical');
SELECT * FROM citus_shards WHERE nodeport IN (:worker_1_port, :worker_2_port);

\c - - - :worker_2_port
SELECT slot_name FROM pg_replication_slots;