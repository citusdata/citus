
SET citus.next_shard_id TO 610000;

-- ===================================================================
-- test ddl command generation functionality
-- ===================================================================

-- first make sure a simple table works
CREATE TABLE simple_table (
	first_name text,
	last_name text,
	id bigint
);

SELECT master_get_table_ddl_events('simple_table');

-- ensure not-null constraints are propagated
CREATE TABLE not_null_table (
	city text,
	id bigint not null
);

SELECT master_get_table_ddl_events('not_null_table');

-- even more complex constraints should be preserved...
CREATE TABLE column_constraint_table (
	first_name text,
	last_name text,
	age int CONSTRAINT non_negative_age CHECK (age >= 0)
);

SELECT master_get_table_ddl_events('column_constraint_table');

-- including table constraints
CREATE TABLE table_constraint_table (
	bid_item_id bigint,
	min_bid decimal not null,
	max_bid decimal not null,
	CONSTRAINT bids_ordered CHECK (min_bid > max_bid)
);

SELECT master_get_table_ddl_events('table_constraint_table');

-- tables with "simple" CHECK constraints should be able to be distributed

CREATE TABLE check_constraint_table_1(
	id int,
	b boolean,
	CHECK(b)
);

SELECT create_distributed_table('check_constraint_table_1', 'id');

SELECT master_get_table_ddl_events('check_constraint_table_1');

-- including hardcoded Booleans
CREATE TABLE check_constraint_table_2(
	id int CHECK(true)
);

SELECT create_distributed_table('check_constraint_table_2', 'id');

SELECT master_get_table_ddl_events('check_constraint_table_2');

-- default values are supported
CREATE TABLE default_value_table (
	name text,
	price decimal default 0.00
);

SELECT master_get_table_ddl_events('default_value_table');

-- of course primary keys work...
CREATE TABLE pkey_table (
	first_name text,
	last_name text,
	id bigint PRIMARY KEY
);

SELECT master_get_table_ddl_events('pkey_table');

-- as do unique indexes...
CREATE TABLE unique_table (
	user_id bigint not null,
	username text UNIQUE not null
);

SELECT master_get_table_ddl_events('unique_table');

-- and indexes used for clustering
CREATE TABLE clustered_table (
	data json not null,
	received_at timestamp not null
);

CREATE INDEX clustered_time_idx ON clustered_table (received_at);

CLUSTER clustered_table USING clustered_time_idx;

SELECT master_get_table_ddl_events('clustered_table');

-- fiddly things like storage type and statistics also work
CREATE TABLE fiddly_table (
	hostname char(255) not null,
	os char(255) not null,
	ip_addr inet not null,
	traceroute text not null
);

ALTER TABLE fiddly_table
	ALTER hostname SET STORAGE PLAIN,
	ALTER os SET STORAGE MAIN,
	ALTER ip_addr SET STORAGE EXTENDED,
	ALTER traceroute SET STORAGE EXTERNAL,
	ALTER ip_addr SET STATISTICS 500;

SELECT master_get_table_ddl_events('fiddly_table');

-- propagating views is not supported if local table dependency exists
CREATE VIEW local_view AS SELECT * FROM simple_table;

SELECT master_get_table_ddl_events('local_view');

-- clean up
DROP VIEW IF EXISTS local_view;
DROP TABLE IF EXISTS simple_table, not_null_table, column_constraint_table,
					 table_constraint_table, default_value_table, pkey_table,
					 unique_table, clustered_table, fiddly_table;
