--
-- GRANT_ON_SEQUENCE_PROPAGATION
--
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA grant_on_sequence;
SET search_path TO grant_on_sequence, public;

-- create some simple sequences
CREATE SEQUENCE dist_seq_0;
CREATE SEQUENCE dist_seq_1;
CREATE SEQUENCE non_dist_seq_0;

-- create some users and grant them permission on grant_on_sequence schema
CREATE USER seq_user_0;
CREATE USER seq_user_1;
CREATE USER seq_user_2;
GRANT ALL ON SCHEMA grant_on_sequence TO seq_user_0, seq_user_1, seq_user_2;

-- do some varying grants
GRANT SELECT ON SEQUENCE dist_seq_0 TO seq_user_0;
GRANT USAGE ON SEQUENCE dist_seq_0 TO seq_user_1 WITH GRANT OPTION;
SET ROLE seq_user_1;
GRANT USAGE ON SEQUENCE dist_seq_0 TO seq_user_2;
RESET ROLE;

-- distribute a sequence
-- reminder: a sequence is distributed when used in a distributed table AND cluster has metadata workers
CREATE TABLE seq_test_0 (a int, b bigint DEFAULT nextval('dist_seq_0'));
SELECT create_distributed_table('seq_test_0', 'a');
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- check grants propagated correctly after sequence is distributed
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- do some varying revokes
REVOKE SELECT ON SEQUENCE dist_seq_0 FROM seq_user_0, seq_user_2;
REVOKE GRANT OPTION FOR USAGE ON SEQUENCE dist_seq_0 FROM seq_user_1 CASCADE;

-- check revokes propagated correctly for the distributed sequence dist_seq_0
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

REVOKE USAGE ON SEQUENCE dist_seq_0 FROM seq_user_1;

SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_0' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- distribute another sequence
CREATE TABLE seq_test_1 (a int, b bigint DEFAULT nextval('dist_seq_1'));
SELECT create_distributed_table('seq_test_1', 'a');

-- GRANT .. ON ALL SEQUENCES IN SCHEMA .. with multiple roles
GRANT ALL ON ALL SEQUENCES IN SCHEMA grant_on_sequence TO seq_user_0, seq_user_2;

SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1') ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1') ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- REVOKE .. ON ALL SEQUENCES IN SCHEMA .. with multiple roles
REVOKE ALL ON ALL SEQUENCES IN SCHEMA grant_on_sequence FROM seq_user_0, seq_user_2;

SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1') ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1') ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- GRANT with multiple sequences and multiple roles
GRANT UPDATE ON SEQUENCE dist_seq_0, dist_seq_1, non_dist_seq_0 TO seq_user_1 WITH GRANT OPTION;
SET ROLE seq_user_1;
GRANT UPDATE ON SEQUENCE dist_seq_0, dist_seq_1, non_dist_seq_0 TO seq_user_0, seq_user_2;
RESET ROLE;

SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1', 'non_dist_seq_0') ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1', 'non_dist_seq_0') ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- sync metadata to another node
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

-- check if the grants are propagated correctly
SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1', 'non_dist_seq_0') ORDER BY 1;
\c - - - :worker_2_port
SELECT relname, relacl FROM pg_class WHERE relname IN ('dist_seq_0', 'dist_seq_1', 'non_dist_seq_0') ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- check that it works correctly with a user that is not distributed
CREATE SEQUENCE dist_seq_2;
ALTER TABLE seq_test_1 ALTER COLUMN b SET DEFAULT nextval('dist_seq_2');

SET citus.enable_ddl_propagation TO off;
CREATE USER not_propagated_sequence_user_4;
SET citus.enable_ddl_propagation TO on;

-- when running below command, not_propagated_sequence_user_4 should be propagated
-- to the worker nodes as part of dist_seq_2's dependencies
GRANT USAGE ON sequence dist_seq_2 TO seq_user_0, not_propagated_sequence_user_4;

-- check if the grants are propagated correctly
-- check that we can see the not_propagated_sequence_user_4
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_2' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_2' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- the following should fail as in plain PG
GRANT USAGE ON sequence dist_seq_0, non_existent_sequence TO seq_user_0;
GRANT UPDATE ON sequence dist_seq_0 TO seq_user_0, non_existent_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA grant_on_sequence, non_existent_schema TO seq_user_0;

-- check that GRANT ON TABLE that redirects to sequences works properly
CREATE SEQUENCE dist_seq_3;
ALTER TABLE seq_test_1 ALTER COLUMN b SET DEFAULT nextval('dist_seq_3');

GRANT UPDATE ON TABLE seq_test_1, dist_seq_3 TO seq_user_0;

SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_3' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_3' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

REVOKE ALL ON TABLE seq_test_1, dist_seq_3 FROM seq_user_0;
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_3' ORDER BY 1;
\c - - - :worker_1_port
SELECT relname, relacl FROM pg_class WHERE relname = 'dist_seq_3' ORDER BY 1;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = grant_on_sequence, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

DROP SCHEMA grant_on_sequence CASCADE;
SET search_path TO public;
