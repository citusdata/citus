SET citus.next_shard_id TO 1730000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;
SET citus.enable_schema_based_sharding TO off;

SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

CREATE USER tenantuser superuser;
SET role tenantuser;

-- check invalid input
SELECT citus_schema_distribute(1);
SELECT citus_schema_undistribute(1);

-- noop
SELECT citus_schema_distribute(null);
SELECT citus_schema_undistribute(null);

-- public and some others cannot be distributed as a tenant schema, but check what happens
-- if we try to call citus_schema_undistribute() for such a schema.
SELECT citus_schema_undistribute('public');

-- create non-tenant schema
CREATE SCHEMA citus_schema_distribute_undistribute;

-- create tenant schema
CREATE SCHEMA tenant1;

CREATE TABLE tenant1.table1(id int PRIMARY KEY, name text);
INSERT INTO tenant1.table1 SELECT i, 'asd'::text FROM generate_series(1,20) i;

CREATE TABLE tenant1.table2(id int REFERENCES tenant1.table1(id), num bigint UNIQUE);
INSERT INTO tenant1.table2 SELECT i, i FROM generate_series(1,20) i;

CREATE TABLE citus_schema_distribute_undistribute.ref(id int PRIMARY KEY);
SELECT create_reference_table('citus_schema_distribute_undistribute.ref');
INSERT INTO citus_schema_distribute_undistribute.ref SELECT i FROM generate_series(1,100) i;

-- autoconverted to Citus local table due to foreign key to reference table
CREATE TABLE tenant1.table3(id int REFERENCES citus_schema_distribute_undistribute.ref(id));
INSERT INTO tenant1.table3 SELECT i FROM generate_series(1,100) i;

-- Citus local table with autoconverted=false
CREATE TABLE tenant1.table3x(id int PRIMARY KEY REFERENCES citus_schema_distribute_undistribute.ref(id));
SELECT citus_add_local_table_to_metadata('tenant1.table3x');
INSERT INTO tenant1.table3x SELECT i FROM generate_series(1,100) i;

-- foreign key to another local table in the same schema
CREATE TABLE tenant1.table3y(id int PRIMARY KEY REFERENCES tenant1.table3x(id));
SELECT citus_add_local_table_to_metadata('tenant1.table3y');
INSERT INTO tenant1.table3y SELECT i FROM generate_series(1,100) i;

-- table with composite type
CREATE TYPE tenant1.catname AS ENUM ('baby', 'teen', 'mid');
CREATE TYPE tenant1.agecat AS (below_age int, name tenant1.catname);
CREATE TABLE tenant1.table4(id int, age tenant1.agecat);

-- create autoconverted partitioned table
CREATE TABLE tenant1.partitioned_table(id int REFERENCES citus_schema_distribute_undistribute.ref(id)) PARTITION BY RANGE(id);
CREATE TABLE tenant1.partition1 PARTITION OF tenant1.partitioned_table FOR VALUES FROM (1) TO (11);
CREATE TABLE tenant1.partition2 PARTITION OF tenant1.partitioned_table FOR VALUES FROM (11) TO (21);
INSERT INTO tenant1.partitioned_table SELECT i FROM generate_series(1,20) i;

-- create view
CREATE VIEW tenant1.view1 AS SELECT * FROM tenant1.table1 JOIN tenant1.table2 USING(id);

-- create view in regular schema
CREATE VIEW citus_schema_distribute_undistribute.view2 AS SELECT * FROM tenant1.view1;

-- create materialized view
CREATE MATERIALIZED VIEW tenant1.view2 AS SELECT * FROM tenant1.table1;

-- create collation
CREATE COLLATION citus_schema_distribute_undistribute.german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

-- create type
CREATE TYPE citus_schema_distribute_undistribute.pair_type AS (a int, b int);

-- Create function
CREATE FUNCTION citus_schema_distribute_undistribute.one_as_result() RETURNS INT LANGUAGE SQL AS
$$
  SELECT 1;
$$;

-- create text search dictionary
CREATE TEXT SEARCH DICTIONARY citus_schema_distribute_undistribute.my_german_dict (
    template = snowball,
    language = german,
    stopwords = german
);

-- create text search config
CREATE TEXT SEARCH CONFIGURATION citus_schema_distribute_undistribute.my_ts_config ( parser = default );
ALTER TEXT SEARCH CONFIGURATION citus_schema_distribute_undistribute.my_ts_config ALTER MAPPING FOR asciiword WITH citus_schema_distribute_undistribute.my_german_dict;

-- create sequence
CREATE SEQUENCE citus_schema_distribute_undistribute.seq;

-- create complex table
CREATE TABLE tenant1.complextable (id int PRIMARY KEY default nextval('citus_schema_distribute_undistribute.seq'), col int default (citus_schema_distribute_undistribute.one_as_result()), myserial serial, phone text COLLATE citus_schema_distribute_undistribute.german_phonebook, initials citus_schema_distribute_undistribute.pair_type);
CREATE SEQUENCE tenant1.seq_owned OWNED BY tenant1.complextable.id;

-- not allowed from workers
SELECT run_command_on_workers($$SELECT citus_schema_distribute('tenant1');$$);
SELECT run_command_on_workers($$SELECT citus_schema_undistribute('tenant1');$$);

-- inherited table not allowed
CREATE TABLE citus_schema_distribute_undistribute.cities (
  name       text,
  population real,
  elevation  int
);
CREATE TABLE citus_schema_distribute_undistribute.capitals (
  state      char(2) UNIQUE NOT NULL
) INHERITS (citus_schema_distribute_undistribute.cities);

-- temporarily move "cities" into tenant1 (not a tenant schema yet) to test citus_schema_distribute() with a table that is inherited
ALTER TABLE citus_schema_distribute_undistribute.cities SET SCHEMA tenant1;
SELECT citus_schema_distribute('tenant1');
ALTER TABLE tenant1.cities SET SCHEMA citus_schema_distribute_undistribute;

-- temporarily move "capitals" into tenant1 (not a tenant schema yet) to test citus_schema_distribute() with a table that inherits
ALTER TABLE citus_schema_distribute_undistribute.capitals SET SCHEMA tenant1;
SELECT citus_schema_distribute('tenant1');
ALTER TABLE tenant1.capitals SET SCHEMA citus_schema_distribute_undistribute;

SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

CREATE TABLE citus_schema_distribute_undistribute.illegal_partitioned_table(id int) PARTITION BY RANGE(id);
CREATE TABLE citus_schema_distribute_undistribute.illegal_partition1 PARTITION OF citus_schema_distribute_undistribute.illegal_partitioned_table FOR VALUES FROM (1) TO (11);

-- temporarily move "illegal_partitioned_table" into tenant1 (not a tenant schema yet) to test citus_schema_distribute() with a partition table whose parent is created in another schema
ALTER TABLE citus_schema_distribute_undistribute.illegal_partitioned_table SET SCHEMA tenant1;
SELECT citus_schema_distribute('tenant1');
ALTER TABLE tenant1.illegal_partitioned_table SET SCHEMA citus_schema_distribute_undistribute;

-- temporarily move "illegal_partition1" into tenant1 (not a tenant schema yet) to test citus_schema_distribute() with a parent table whose partition is created in another schema
ALTER TABLE citus_schema_distribute_undistribute.illegal_partition1 SET SCHEMA tenant1;
SELECT citus_schema_distribute('tenant1');
ALTER TABLE tenant1.illegal_partition1 SET SCHEMA citus_schema_distribute_undistribute;

SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- foreign key to a local table in another schema is not allowed
CREATE TABLE citus_schema_distribute_undistribute.tbl1(id int PRIMARY KEY);
CREATE TABLE tenant1.table3z(id int PRIMARY KEY REFERENCES citus_schema_distribute_undistribute.tbl1(id));
SELECT citus_schema_distribute('tenant1');
SELECT create_reference_table('citus_schema_distribute_undistribute.tbl1');
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- foreign key to a distributed table in another schema is not allowed
CREATE TABLE citus_schema_distribute_undistribute.tbl2(id int PRIMARY KEY);
SELECT create_distributed_table('citus_schema_distribute_undistribute.tbl2','id');
CREATE TABLE tenant1.table3w(id int PRIMARY KEY REFERENCES citus_schema_distribute_undistribute.tbl2(id));
SELECT citus_schema_distribute('tenant1');
DROP TABLE citus_schema_distribute_undistribute.tbl2 CASCADE;
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- foreign key from a local table in another schema is not allowed
CREATE TABLE tenant1.table3q(id int PRIMARY KEY);
CREATE TABLE citus_schema_distribute_undistribute.tbl3(id int PRIMARY KEY REFERENCES tenant1.table3q(id));
SELECT citus_schema_distribute('tenant1');
DROP TABLE citus_schema_distribute_undistribute.tbl3 CASCADE;
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- foreign key from a reference table in another schema is not allowed
CREATE TABLE tenant1.table3t(id int PRIMARY KEY);
CREATE TABLE citus_schema_distribute_undistribute.tbl4(id int PRIMARY KEY REFERENCES tenant1.table3t(id));
SELECT create_reference_table('citus_schema_distribute_undistribute.tbl4');
SELECT citus_schema_distribute('tenant1');
DROP TABLE citus_schema_distribute_undistribute.tbl4 CASCADE;
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- only allowed for schema owner or superuser
CREATE USER dummyregular;
SET role dummyregular;
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- assign all tables to dummyregular except table5
SET role tenantuser;
SELECT result FROM run_command_on_all_nodes($$ REASSIGN OWNED BY tenantuser TO dummyregular; $$);
CREATE TABLE tenant1.table5(id int);

-- table owner check fails the distribution
SET role dummyregular;
SELECT citus_schema_distribute('tenant1');

-- alter table owner, then redistribute
SET role tenantuser;
ALTER TABLE tenant1.table5 OWNER TO dummyregular;
SET role dummyregular;
SELECT citus_schema_distribute('tenant1');

-- show the schema is a tenant schema now
SELECT colocationid AS tenant1_colocid FROM pg_dist_schema schemaid \gset
-- below query verifies the same colocationid in pg_dist_schema, pg_dist_colocation and all entries in pg_dist_partition at the same time
SELECT '$$' ||
       ' SELECT colocationid = ' || :tenant1_colocid ||
       ' FROM pg_dist_schema JOIN pg_dist_colocation USING(colocationid)' ||
       ' WHERE colocationid = ALL(SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE ''tenant1%'')' ||
       '$$'
AS verify_tenant_query \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*) AS total FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1%' $$);
SELECT result FROM run_command_on_all_nodes(:verify_tenant_query);

SELECT citus_schema_undistribute('tenant1');

-- show the schema is a regular schema
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
-- below query verifies the tenant colocationid is removed from both pg_dist_colocation and all entries in pg_dist_partition at the same time
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

RESET role;
SELECT result FROM run_command_on_all_nodes($$ REASSIGN OWNED BY dummyregular TO tenantuser; $$);
DROP USER dummyregular;

CREATE USER dummysuper superuser;
SET role dummysuper;
SELECT citus_schema_distribute('tenant1');

-- show the schema is a tenant schema now
SELECT colocationid AS tenant1_colocid FROM pg_dist_schema schemaid \gset
SELECT '$$' ||
       ' SELECT colocationid = ' || :tenant1_colocid ||
       ' FROM pg_dist_schema JOIN pg_dist_colocation USING(colocationid)' ||
       ' WHERE colocationid = ALL(SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE ''tenant1%'')' ||
       '$$'
AS verify_tenant_query \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*) AS total FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1%' $$);
SELECT result FROM run_command_on_all_nodes(:verify_tenant_query);

SELECT citus_schema_undistribute('tenant1');

-- show the schema is a regular schema
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

RESET role;
DROP USER dummysuper;

-- foreign table
CREATE TABLE tenant1.foreign_table_test (id integer NOT NULL, data text, a bigserial);
INSERT INTO tenant1.foreign_table_test SELECT i FROM generate_series(1,100) i;
CREATE EXTENSION postgres_fdw;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');
CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server
        OPTIONS (user 'postgres');
CREATE FOREIGN TABLE tenant1.foreign_table (
        id integer NOT NULL,
        data text,
        a bigserial
)
        SERVER foreign_server
        OPTIONS (schema_name 'tenant1', table_name 'foreign_table_test');

-- foreign table not allowed
SELECT citus_schema_distribute('tenant1');
ALTER FOREIGN TABLE tenant1.foreign_table SET SCHEMA citus_schema_distribute_undistribute;
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- already have distributed table error
CREATE TABLE tenant1.dist(id int);
SELECT create_distributed_table('tenant1.dist', 'id');
SELECT citus_schema_distribute('tenant1');
SELECT undistribute_table('tenant1.dist');
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

CREATE TABLE tenant1.ref2(id int);
SELECT create_reference_table('tenant1.ref2');
SELECT citus_schema_distribute('tenant1');
SELECT undistribute_table('tenant1.ref2');
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

BEGIN;
SELECT citus_schema_distribute('tenant1');
ROLLBACK;

-- show the schema is a regular schema
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

-- errors not a tenant schema
SELECT citus_schema_undistribute('tenant1');

-- make it a tenant schema
BEGIN;
SELECT citus_schema_distribute('tenant1');
COMMIT;

-- show the schema is a tenant schema now
SELECT colocationid AS tenant1_colocid FROM pg_dist_schema schemaid \gset
SELECT '$$' ||
       ' SELECT colocationid = ' || :tenant1_colocid ||
       ' FROM pg_dist_schema JOIN pg_dist_colocation USING(colocationid)' ||
       ' WHERE colocationid = ALL(SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE ''tenant1%'')' ||
       '$$'
AS verify_tenant_query \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*) AS total FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1%' $$);
SELECT result FROM run_command_on_all_nodes(:verify_tenant_query);

-- already a tenant schema notice
SET client_min_messages TO NOTICE;
SELECT citus_schema_distribute('tenant1');
SET client_min_messages TO WARNING;

-- convert back to a regular schema
SELECT citus_schema_undistribute('tenant1');

-- show the schema is a regular schema now
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

-- tables still have valid data
SELECT COUNT(*) FROM tenant1.partitioned_table;
SELECT COUNT(*) FROM citus_schema_distribute_undistribute.foreign_table;
SELECT COUNT(*) FROM tenant1.table3;
TRUNCATE citus_schema_distribute_undistribute.ref CASCADE;
SELECT COUNT(*) FROM tenant1.table3;

-- disallowed namespaces
SELECT citus_schema_distribute('public');
SELECT citus_schema_distribute('pg_catalog');
SELECT citus_schema_distribute('pg_toast');
CREATE TEMP TABLE xx(id int); -- create a temp table in case we do not have any pg_temp_xx schema yet
SELECT nspname AS temp_schema_name FROM pg_namespace WHERE nspname LIKE 'pg_temp%' LIMIT 1 \gset
SELECT nspname AS temp_toast_schema_name FROM pg_namespace WHERE nspname LIKE 'pg_toast_temp%' LIMIT 1 \gset
SELECT citus_schema_distribute(:'temp_schema_name');
SELECT citus_schema_distribute(:'temp_toast_schema_name');
SELECT citus_schema_distribute('citus');
CREATE SCHEMA extensionschema;
CREATE EXTENSION citext SCHEMA extensionschema;
SELECT citus_schema_distribute('extensionschema');
DROP SCHEMA extensionschema CASCADE;
ALTER EXTENSION citus ADD TABLE tenant1.table1;
SELECT citus_schema_distribute('tenant1');
ALTER EXTENSION citus DROP TABLE tenant1.table1;

-- weird schema and table names
CREATE SCHEMA "CiTuS.TeeN";
CREATE TABLE "CiTuS.TeeN"."TeeNTabLE.1!?!"(id int PRIMARY KEY, name text);
INSERT INTO "CiTuS.TeeN"."TeeNTabLE.1!?!" SELECT i, 'asd'::text FROM generate_series(1,20) i;
SELECT citus_schema_distribute('"CiTuS.TeeN"');

-- show the schema is a tenant schema now
SELECT colocationid AS tenant1_colocid FROM pg_dist_schema schemaid \gset
SELECT '$$' ||
       ' SELECT colocationid = ' || :tenant1_colocid ||
       ' FROM pg_dist_schema JOIN pg_dist_colocation USING(colocationid)' ||
       ' WHERE colocationid = ALL(SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE ''"CiTuS.TeeN"%'')' ||
       '$$'
AS verify_tenant_query \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*) AS total FROM pg_dist_partition WHERE logicalrelid::text LIKE '"CiTuS.TeeN"%' $$);
SELECT result FROM run_command_on_all_nodes(:verify_tenant_query);

SELECT citus_schema_undistribute('"CiTuS.TeeN"');

-- show the schema is a regular schema
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

-- try setting the schema again after adding a distributed table into the schema. It should complain about distributed table.
CREATE TABLE tenant1.new_dist(id int);
SELECT create_distributed_table('tenant1.new_dist', 'id');
SELECT citus_schema_distribute('tenant1');
SELECT undistribute_table('tenant1.new_dist');
SELECT citus_schema_distribute('tenant1');
SELECT citus_schema_undistribute('tenant1');

-- try setting the schema again after adding a single shard table into the schema. It should complain about distributed table.
CREATE TABLE tenant1.single_shard_t(id int);
SELECT create_distributed_table('tenant1.single_shard_t', NULL);
SELECT citus_schema_distribute('tenant1');
SELECT undistribute_table('tenant1.single_shard_t');

-- try setting the schema again. It should succeed now.
SELECT citus_schema_distribute('tenant1');

-- show the schema is a tenant schema now
SELECT colocationid AS tenant1_colocid FROM pg_dist_schema schemaid \gset
SELECT '$$' ||
       ' SELECT colocationid = ' || :tenant1_colocid ||
       ' FROM pg_dist_schema JOIN pg_dist_colocation USING(colocationid)' ||
       ' WHERE colocationid = ALL(SELECT colocationid FROM pg_dist_partition WHERE logicalrelid::text LIKE ''tenant1%'')' ||
       '$$'
AS verify_tenant_query \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*) AS total FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1%' $$);
SELECT result FROM run_command_on_all_nodes(:verify_tenant_query);

SELECT citus_schema_undistribute('tenant1');

-- show the schema is a regular schema now
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT COUNT(*)=0 FROM pg_dist_colocation FULL JOIN pg_dist_partition USING(colocationid) WHERE (logicalrelid::text LIKE 'tenant1.%' OR logicalrelid is NULL) AND colocationid > 0 $$);
SELECT result FROM run_command_on_all_nodes($$ SELECT array_agg(logicalrelid ORDER BY logicalrelid) FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%' AND colocationid > 0 $$);

-- create an empty tenant schema to verify colocation id is removed successfully after we undistribute it
CREATE SCHEMA empty_tenant;
SELECT citus_schema_distribute('empty_tenant');

-- show the schema is a tenant schema now
SELECT colocationid AS empty_tenant_colocid FROM pg_dist_schema schemaid \gset
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace FROM pg_dist_colocation JOIN pg_dist_schema USING(colocationid) $$);

SELECT citus_schema_undistribute('empty_tenant');

-- show the schema is a regular schema now
SELECT result FROM run_command_on_all_nodes($$ SELECT schemaid::regnamespace as schemaname FROM pg_dist_schema $$);
SELECT '$$' || 'SELECT COUNT(*)=0 FROM pg_dist_colocation WHERE colocationid = ' || :empty_tenant_colocid || '$$'
AS verify_empty_tenant_query \gset
SELECT result FROM run_command_on_all_nodes(:verify_empty_tenant_query);

-- cleanup
DROP SCHEMA "CiTuS.TeeN" CASCADE;
DROP SCHEMA tenant1 CASCADE;
DROP SCHEMA empty_tenant CASCADE;
DROP EXTENSION postgres_fdw CASCADE;
DROP SCHEMA citus_schema_distribute_undistribute CASCADE;
DROP USER tenantuser;
SELECT citus_remove_node('localhost', :master_port);
