CREATE SCHEMA worker_split_copy_test;
SET search_path TO worker_split_copy_test;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 81070000;

-- BEGIN: Create distributed table and insert data.

CREATE TABLE worker_split_copy_test."test !/ \n _""dist_123_table"(id int primary key, value char);
SELECT create_distributed_table('"test !/ \n _""dist_123_table"', 'id');

INSERT INTO "test !/ \n _""dist_123_table" (id, value) (SELECT g.id, 'N' FROM generate_series(1, 1000) AS g(id));

-- END: Create distributed table and insert data.

-- BEGIN: Switch to Worker1, Create target shards in worker for local 2-way split copy.
\c - - - :worker_1_port
CREATE TABLE worker_split_copy_test."test !/ \n _""dist_123_table_81070015"(id int primary key, value char);
CREATE TABLE worker_split_copy_test."test !/ \n _""dist_123_table_81070016"(id int primary key, value char);
-- End: Switch to Worker1, Create target shards in worker for local 2-way split copy.

-- BEGIN: List row count for source shard and targets shard in Worker1.
\c - - - :worker_1_port
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070000";
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070015";
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070016";

\c - - - :worker_2_port
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070001";
-- END: List row count for source shard and targets shard in Worker1.

-- BEGIN: Set worker_1_node and worker_2_node
\c - - - :worker_1_port
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
-- END: Set worker_1_node and worker_2_node

-- BEGIN: Test Negative scenario
SELECT * from worker_split_copy(
    101, -- Invalid source shard id.
    ARRAY[
         -- split copy info for split children 1
        ROW(81070015, -- destination shard id
             -2147483648, -- split range begin
            -1073741824, --split range end
            :worker_1_node)::pg_catalog.split_copy_info,
        -- split copy info for split children 2
        ROW(81070016,  --destination shard id
            -1073741823, --split range begin
            -1, --split range end
            :worker_1_node)::pg_catalog.split_copy_info
        ]
    );

SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[] -- empty array
    );

SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[NULL] -- empty array
    );

SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[NULL::pg_catalog.split_copy_info]-- empty array
    );

SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[ROW(NULL)]-- empty array
    );

SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[ROW(NULL, NULL, NULL, NULL)::pg_catalog.split_copy_info] -- empty array
    );
-- END: Test Negative scenario

-- BEGIN: Trigger 2-way local shard split copy.
-- Ensure we will perform text copy.
SET citus.enable_binary_protocol = false;
SELECT * from worker_split_copy(
    81070000, -- source shard id to copy
    ARRAY[
         -- split copy info for split children 1
        ROW(81070015, -- destination shard id
             -2147483648, -- split range begin
            -1073741824, --split range end
            :worker_1_node)::pg_catalog.split_copy_info,
        -- split copy info for split children 2
        ROW(81070016,  --destination shard id
            -1073741823, --split range begin
            -1, --split range end
            :worker_1_node)::pg_catalog.split_copy_info
        ]
    );
-- END: Trigger 2-way local shard split copy.

-- BEGIN: List updated row count for local targets shard.
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070015";
SELECT COUNT(*) FROM worker_split_copy_test."test !/ \n _""dist_123_table_81070016";
-- END: List updated row count for local targets shard.

-- BEGIN: CLEANUP.
\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA worker_split_copy_test CASCADE;
-- END: CLEANUP.
