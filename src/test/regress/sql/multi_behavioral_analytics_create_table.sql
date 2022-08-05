--
-- multi behavioral analytics
-- this file is intended to create the table requires for the tests
--
SET citus.next_shard_id TO 1400000;
SET citus.shard_replication_factor = 1;
SET citus.shard_count = 32;

CREATE SCHEMA with_basics;
SET search_path TO 'with_basics';

CREATE TABLE users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('users_table', 'user_id');

CREATE TABLE events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('events_table', 'user_id');

\set users_table_data_file :abs_srcdir '/data/users_table.data'
\set events_table_data_file :abs_srcdir '/data/events_table.data'
COPY users_table FROM :'users_table_data_file' WITH CSV;
COPY events_table FROM :'events_table_data_file' WITH CSV;

SET citus.shard_count = 96;
CREATE SCHEMA subquery_and_ctes;
SET search_path TO subquery_and_ctes;

CREATE TABLE users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('users_table', 'user_id');

CREATE TABLE events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('events_table', 'user_id');

COPY users_table FROM :'users_table_data_file' WITH CSV;
COPY events_table FROM :'events_table_data_file' WITH CSV;

SET citus.shard_count TO DEFAULT;
SET search_path TO DEFAULT;

CREATE TABLE users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('users_table', 'user_id');

CREATE TABLE events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint);
SELECT create_distributed_table('events_table', 'user_id');

CREATE TABLE agg_results (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp);
SELECT create_distributed_table('agg_results', 'user_id');

-- we need this to improve the concurrency on the regression tests
CREATE TABLE agg_results_second (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp);
SELECT create_distributed_table('agg_results_second', 'user_id');

-- same as agg_results_second
CREATE TABLE agg_results_third (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp);
SELECT create_distributed_table('agg_results_third', 'user_id');

-- same as agg_results_second
CREATE TABLE agg_results_fourth (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp);
SELECT create_distributed_table('agg_results_fourth', 'user_id');

-- same as agg_results_second
CREATE TABLE agg_results_window (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp);
SELECT create_distributed_table('agg_results_window', 'user_id');

CREATE TABLE users_ref_test_table(id int, it_name varchar(25), k_no int);
SELECT create_reference_table('users_ref_test_table');
INSERT INTO users_ref_test_table VALUES(1,'User_1',45);
INSERT INTO users_ref_test_table VALUES(2,'User_2',46);
INSERT INTO users_ref_test_table VALUES(3,'User_3',47);
INSERT INTO users_ref_test_table VALUES(4,'User_4',48);
INSERT INTO users_ref_test_table VALUES(5,'User_5',49);
INSERT INTO users_ref_test_table VALUES(6,'User_6',50);

COPY users_table FROM :'users_table_data_file' WITH CSV;
COPY events_table FROM :'events_table_data_file' WITH CSV;

-- create indexes for
CREATE INDEX is_index1 ON users_table(user_id);
CREATE INDEX is_index2 ON events_table(user_id);
CREATE INDEX is_index3 ON users_table(value_1);
CREATE INDEX is_index4 ON events_table(event_type);
CREATE INDEX is_index5 ON users_table(value_2);
CREATE INDEX is_index6 ON events_table(value_2);

-- Create composite type to use in subquery pushdown
CREATE TYPE user_composite_type AS
(
    tenant_id BIGINT,
    user_id BIGINT
);


-- ... create a test HASH function. Though it is a poor hash function,
-- it is acceptable for our tests
SELECT run_command_on_master_and_workers($f$

	CREATE FUNCTION test_composite_type_hash(user_composite_type) RETURNS int
	AS 'SELECT hashtext( ($1.tenant_id + $1.tenant_id)::text);'
	LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;
$f$);

SET citus.next_shard_id TO 1400297;

CREATE TABLE events_reference_table (like events_table including all);
SELECT create_reference_table('events_reference_table');
CREATE INDEX events_ref_val2 on events_reference_table(value_2);
INSERT INTO events_reference_table SELECT * FROM events_table;

CREATE TABLE users_reference_table (like users_table including all);
SELECT create_reference_table('users_reference_table');
INSERT INTO users_reference_table SELECT * FROM users_table;
