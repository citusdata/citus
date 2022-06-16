--
-- MULTI_UTILITY_WARNINGS
--

-- Tests to check if we inform the user about potential caveats of creating new
-- databases, schemas.


SET citus.next_shard_id TO 1010000;


CREATE DATABASE new_database;

BEGIN;
INSERT INTO pg_dist_node VALUES (1234567890, 1234567890, 'localhost', 5432);
INSERT INTO pg_dist_poolinfo VALUES (1234567890, 'port=1234');
ROLLBACK;
