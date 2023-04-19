SET citus.next_shard_id TO 1730000;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO WARNING;
SET citus.enable_schema_based_sharding TO off;

CREATE SCHEMA tenant1;

SELECT citus_set_coordinator_host('localhost', :master_port);

CREATE TABLE tenant1.table1(id int PRIMARY KEY, name text);
INSERT INTO tenant1.table1 SELECT i, 'asd'::text FROM generate_series(1,20) i;

CREATE TABLE tenant1.table2(id int REFERENCES tenant1.table1(id), num bigint UNIQUE);
INSERT INTO tenant1.table2 SELECT i, i FROM generate_series(1,20) i;

CREATE TABLE public.ref(id int PRIMARY KEY);
SELECT create_reference_table('public.ref');
INSERT INTO public.ref SELECT i FROM generate_series(1,100) i;

-- autoconverted to Citus local table due to foreign key to reference table
CREATE TABLE tenant1.table3(id int REFERENCES public.ref(id));
INSERT INTO tenant1.table3 SELECT i FROM generate_series(1,100) i;

-- table with composite type
CREATE TYPE tenant1.catname AS ENUM ('baby', 'teen', 'mid');
CREATE TYPE tenant1.agecat AS (below_age int, name tenant1.catname);
CREATE TABLE tenant1.table4(id int, age tenant1.agecat);

-- create autoconverted partitioned table
CREATE TABLE tenant1.partitioned_table(id int REFERENCES public.ref(id)) PARTITION BY RANGE(id);
CREATE TABLE tenant1.partition1 PARTITION OF tenant1.partitioned_table FOR VALUES FROM (1) TO (11);
CREATE TABLE tenant1.partition2 PARTITION OF tenant1.partitioned_table FOR VALUES FROM (11) TO (21);
INSERT INTO tenant1.partitioned_table SELECT i FROM generate_series(1,20) i;

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

-- create inherited table
CREATE TABLE tenant1.cities (
  name       text,
  population real,
  elevation  int
);

CREATE TABLE tenant1.capitals (
  state      char(2) UNIQUE NOT NULL
) INHERITS (tenant1.cities);

-- create view
CREATE VIEW tenant1.view1 AS SELECT * FROM tenant1.table1 JOIN tenant1.table2 USING(id);

-- create materialized view
CREATE MATERIALIZED VIEW tenant1.view2 AS SELECT * FROM tenant1.table1;

-- only allowed from coordinator
SELECT run_command_on_workers($$SELECT citus_schema_tenant_set('tenant1');$$);
SELECT run_command_on_workers($$SELECT citus_schema_tenant_unset('tenant1');$$);

-- inherited table not allowed
SELECT citus_schema_tenant_set('tenant1');
DROP TABLE tenant1.capitals;
DROP TABLE tenant1.cities;

-- foreign table not allowed
SELECT citus_schema_tenant_set('tenant1');
ALTER FOREIGN TABLE tenant1.foreign_table SET SCHEMA public;

-- only allowed for schema owner or superuser
CREATE USER dummysuper superuser;
CREATE USER dummyregular;
GRANT EXECUTE ON FUNCTION citus_schema_tenant_set TO dummyregular;
SET role 'dummyregular';
SELECT citus_schema_tenant_set('tenant1');
SET role 'dummysuper';
SELECT citus_schema_tenant_set('tenant1');
RESET role;
REVOKE EXECUTE ON FUNCTION citus_schema_tenant_set FROM dummyregular;
DROP USER dummyregular;
DROP USER dummysuper;

SELECT citus_schema_tenant_unset('tenant1');

-- already have distributed table error
CREATE TABLE tenant1.dist(id int);
SELECT create_distributed_table('tenant1.dist', 'id');
SELECT citus_schema_tenant_set('tenant1');
SELECT undistribute_table('tenant1.dist');
CREATE TABLE tenant1.ref2(id int);
SELECT create_reference_table('tenant1.ref2');
SELECT citus_schema_tenant_set('tenant1');
SELECT undistribute_table('tenant1.ref2');

BEGIN;
SELECT citus_schema_tenant_set('tenant1');
ROLLBACK;

-- show the schema is not yet a tenant schema
SELECT schemaid::regnamespace as schemaname, logicalrelid FROM pg_dist_partition JOIN pg_dist_tenant_schema s USING(colocationid) ORDER BY schemaid, logicalrelid::text;

-- errors not a tenant schema
SELECT citus_schema_tenant_unset('tenant1');

-- make it a tenant schema
BEGIN;
SELECT citus_schema_tenant_set('tenant1');
COMMIT;

-- show the schema is a tenant schema now
SELECT schemaid::regnamespace as schemaname, logicalrelid FROM pg_dist_partition JOIN pg_dist_tenant_schema s USING(colocationid) ORDER BY schemaid, logicalrelid::text;

SELECT colocationid = (SELECT colocationid FROM pg_dist_tenant_schema WHERE schemaid::regnamespace::text = 'tenant1')
FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.%';

-- already a tenant schema notice
SET client_min_messages TO NOTICE;
SELECT citus_schema_tenant_set('tenant1');
SET client_min_messages TO WARNING;

-- convert back to a regular schema
SELECT citus_schema_tenant_unset('tenant1');

-- show the schema is back a regular schema after unsetting the tenant
SELECT schemaid::regnamespace as schemaname FROM pg_dist_tenant_schema;
-- show tenant1.table3 is left as single shard table after unsetting tenant schema
SELECT logicalrelid, partmethod, partkey, (colocationid > 0) AS has_non_zero_colocationid, repmodel, autoconverted
FROM pg_dist_partition WHERE logicalrelid::text = 'tenant1.table3';
-- show tenant1.partitioned_table is left as single shard table after unsetting tenant schema
SELECT logicalrelid, partmethod, partkey, (colocationid > 0) AS has_non_zero_colocationid, repmodel, autoconverted
FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant1.partition%';
-- save colocationid after unsetting the tenant schema
SELECT colocationid AS tenant1_colocid FROM pg_dist_partition WHERE logicalrelid::text = 'tenant1.table3' \gset

-- tables still have valid data
SELECT COUNT(*) FROM tenant1.partitioned_table;
SELECT COUNT(*) FROM public.foreign_table;
SELECT COUNT(*) FROM tenant1.table3;
TRUNCATE public.ref CASCADE;
SELECT COUNT(*) FROM tenant1.table3;

-- disallowed namespaces
SELECT citus_schema_tenant_set('public');
SELECT citus_schema_tenant_set('pg_catalog');
SELECT citus_schema_tenant_set('pg_toast');
CREATE TEMP TABLE xx(id int); -- create a temp table in case we do not have any pg_temp_xx schema yet
SELECT nspname AS temp_schema_name FROM pg_namespace WHERE nspname LIKE 'pg_temp%' LIMIT 1 \gset
SELECT nspname AS temp_toast_schema_name FROM pg_namespace WHERE nspname LIKE 'pg_toast_temp%' LIMIT 1 \gset
SELECT citus_schema_tenant_set(:'temp_schema_name');
SELECT citus_schema_tenant_set(:'temp_toast_schema_name');
SELECT citus_schema_tenant_set('citus');

-- weird schema and table names
CREATE SCHEMA "CiTuS.TeeN";
CREATE TABLE "CiTuS.TeeN"."TeeNTabLE.1!?!"(id int PRIMARY KEY, name text);
INSERT INTO "CiTuS.TeeN"."TeeNTabLE.1!?!" SELECT i, 'asd'::text FROM generate_series(1,20) i;
SELECT citus_schema_tenant_set('"CiTuS.TeeN"');
SELECT citus_schema_tenant_unset('"CiTuS.TeeN"');

-- try setting the schema again after adding a distributed table into the schema. It should complain about distributed table.
CREATE TABLE tenant1.new_dist(id int);
SELECT create_distributed_table('tenant1.new_dist', 'id');
SELECT citus_schema_tenant_set('tenant1');
SELECT undistribute_table('tenant1.new_dist');

-- try setting the schema again after adding a non-colocated single shard table into the schema. It should complain about non-colocated shards.
CREATE TABLE tenant1.single_shard_t(id int);
SELECT create_distributed_table('tenant1.single_shard_t', NULL, colocate_with=>'none');
SELECT citus_schema_tenant_set('tenant1');
SELECT undistribute_table('tenant1.single_shard_t');

-- try setting the schema again. It should succeed and colocation id should be same as saved colocationid
SELECT citus_schema_tenant_set('tenant1');
SELECT (colocationid = :tenant1_colocid) AS used_existing_colocid FROM pg_dist_tenant_schema WHERE schemaid::regnamespace::text = 'tenant1';

-- try setting the schema again after removing all single shard tables. It should succeed and colocation id is different than saved colocationid
SELECT citus_schema_tenant_unset('tenant1');
SELECT 'drop table if exists"' || tablename || '" cascade;' FROM pg_tables WHERE schemaname = 'tenant1' ORDER BY tablename;
SELECT citus_schema_tenant_set('tenant1');
SELECT (colocationid = :tenant1_colocid) AS used_existing_colocid FROM pg_dist_tenant_schema WHERE schemaid::regnamespace::text = 'tenant1';
SELECT citus_schema_tenant_unset('tenant1');

-- cleanup
DROP SCHEMA "CiTuS.TeeN" CASCADE;
DROP SCHEMA tenant1 CASCADE;
DROP TABLE public.ref;
DROP EXTENSION postgres_fdw CASCADE;
SELECT citus_remove_node('localhost', :master_port);
