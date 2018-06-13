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
