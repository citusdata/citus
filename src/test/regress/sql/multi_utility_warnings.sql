--
-- MULTI_UTILITY_WARNINGS
--

-- Tests to check if we inform the user about potential caveats of creating new
-- databases, schemas, and roles.


SET citus.next_shard_id TO 1010000;


CREATE DATABASE new_database;

CREATE ROLE new_role;

CREATE USER new_user;

INSERT INTO pg_dist_authinfo VALUES (0, 'new_user', 'password=1234');

BEGIN;
INSERT INTO pg_dist_node VALUES (1234567890, 1234567890, 'localhost', 5432);
INSERT INTO pg_dist_poolinfo VALUES (1234567890, 'port=1234');
ROLLBACK;
INSERT INTO pg_dist_rebalance_strategy VALUES ('should fail', false, 'citus_shard_cost_1', 'citus_node_capacity_1', 'citus_shard_allowed_on_node_true', 0, 0);
