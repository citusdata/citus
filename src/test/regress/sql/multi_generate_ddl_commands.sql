
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

-- test foreign tables using fake FDW
CREATE FOREIGN TABLE foreign_table (
	id bigint not null,
	full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true');

SELECT create_distributed_table('foreign_table', 'id');
ALTER FOREIGN TABLE foreign_table rename to renamed_foreign_table;
ALTER FOREIGN TABLE renamed_foreign_table rename full_name to rename_name;
ALTER FOREIGN TABLE renamed_foreign_table alter rename_name type char(8);
\c - - :public_worker_1_host :worker_1_port
select table_name, column_name, data_type
from information_schema.columns
where table_schema='public' and table_name like 'renamed_foreign_table_%' and column_name <> 'id'
order by table_name;
\c - - :master_host :master_port

SELECT master_get_table_ddl_events('renamed_foreign_table');

-- propagating views is not supported
CREATE VIEW local_view AS SELECT * FROM simple_table;

SELECT master_get_table_ddl_events('local_view');

-- clean up
DROP VIEW IF EXISTS local_view;
DROP FOREIGN TABLE IF EXISTS renamed_foreign_table;
\c - - :public_worker_1_host :worker_1_port
select table_name, column_name, data_type
from information_schema.columns
where table_schema='public' and table_name like 'renamed_foreign_table_%' and column_name <> 'id'
order by table_name;
\c - - :master_host :master_port
DROP TABLE IF EXISTS simple_table, not_null_table, column_constraint_table,
					 table_constraint_table, default_value_table, pkey_table,
					 unique_table, clustered_table, fiddly_table;
