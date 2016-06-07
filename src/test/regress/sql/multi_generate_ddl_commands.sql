
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 610000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 610000;


-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION table_ddl_command_array(regclass)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

-- ===================================================================
-- test ddl command generation functionality
-- ===================================================================

-- first make sure a simple table works
CREATE TABLE simple_table (
	first_name text,
	last_name text,
	id bigint
);

SELECT table_ddl_command_array('simple_table');

-- ensure not-null constraints are propagated
CREATE TABLE not_null_table (
	city text,
	id bigint not null
);

SELECT table_ddl_command_array('not_null_table');

-- ensure tables not in search path are schema-prefixed
CREATE SCHEMA not_in_path CREATE TABLE simple_table (id bigint);

SELECT table_ddl_command_array('not_in_path.simple_table');

-- even more complex constraints should be preserved...
CREATE TABLE column_constraint_table (
	first_name text,
	last_name text,
	age int CONSTRAINT non_negative_age CHECK (age >= 0)
);

SELECT table_ddl_command_array('column_constraint_table');

-- including table constraints
CREATE TABLE table_constraint_table (
	bid_item_id bigint,
	min_bid decimal not null,
	max_bid decimal not null,
	CONSTRAINT bids_ordered CHECK (min_bid > max_bid)
);

SELECT table_ddl_command_array('table_constraint_table');

-- default values are supported
CREATE TABLE default_value_table (
	name text,
	price decimal default 0.00
);

SELECT table_ddl_command_array('default_value_table');

-- of course primary keys work...
CREATE TABLE pkey_table (
	first_name text,
	last_name text,
	id bigint PRIMARY KEY
);

SELECT table_ddl_command_array('pkey_table');

-- as do unique indexes...
CREATE TABLE unique_table (
	user_id bigint not null,
	username text UNIQUE not null
);

SELECT table_ddl_command_array('unique_table');

-- and indexes used for clustering
CREATE TABLE clustered_table (
	data json not null,
	received_at timestamp not null
);

CREATE INDEX clustered_time_idx ON clustered_table (received_at);

CLUSTER clustered_table USING clustered_time_idx;

SELECT table_ddl_command_array('clustered_table');

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

SELECT table_ddl_command_array('fiddly_table');

-- test foreign tables using fake FDW
CREATE FOREIGN TABLE foreign_table (
	id bigint not null,
	full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true');

SELECT table_ddl_command_array('foreign_table');

-- propagating views is not supported
CREATE VIEW local_view AS SELECT * FROM simple_table;

SELECT table_ddl_command_array('local_view');

-- clean up
DROP VIEW IF EXISTS local_view;
DROP FOREIGN TABLE IF EXISTS foreign_table;
DROP TABLE IF EXISTS simple_table, not_null_table, column_constraint_table,
					 table_constraint_table, default_value_table, pkey_table,
					 unique_table, clustered_table, fiddly_table;
