CREATE SCHEMA publication;
CREATE SCHEMA "publication-1";
SET search_path TO publication;
SET citus.shard_replication_factor TO 1;

CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
COMMENT ON FUNCTION activate_node_snapshot()
    IS 'commands to activate node snapshot';

\c - - - :worker_1_port
SET citus.enable_ddl_propagation TO off;

CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
COMMENT ON FUNCTION activate_node_snapshot()
    IS 'commands to activate node snapshot';

\c - - - :worker_2_port
SET citus.enable_ddl_propagation TO off;

CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';
COMMENT ON FUNCTION activate_node_snapshot()
    IS 'commands to activate node snapshot';

-- create some publications with conflicting names on worker node

-- publication will be different from coordinator
CREATE PUBLICATION "pub-all";
-- publication will be same as coordinator
CREATE PUBLICATION "pub-all-insertupdateonly" FOR ALL TABLES WITH (publish = 'insert, update');;

\c - - - :master_port
SET search_path TO publication;
SET citus.shard_replication_factor TO 1;

-- do not create publications on worker 2 initially
SELECT citus_remove_node('localhost', :worker_2_port);

-- create a non-distributed publication
SET citus.enable_ddl_propagation TO off;
CREATE PUBLICATION pubnotdistributed WITH (publish = 'delete');
RESET citus.enable_ddl_propagation;
ALTER PUBLICATION pubnotdistributed SET (publish = 'truncate');

-- create regular, distributed publications
CREATE PUBLICATION pubempty;
CREATE PUBLICATION pubinsertonly WITH (publish = 'insert');
CREATE PUBLICATION "pub-all" FOR ALL TABLES;
CREATE PUBLICATION "pub-all-insertupdateonly" FOR ALL TABLES WITH (publish = 'insert, update');

-- add worker 2 with publications
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- Check publications on all the nodes, if we see the same publication name twice then its definition differs
-- Note that publications are special in the sense that the coordinator object might differ from
-- worker objects due to the presence of regular tables.
SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' ORDER BY 1) s$$)
  ORDER BY c) s;

CREATE TABLE test (x int primary key, y int, "column-1" int, doc xml);
CREATE TABLE "test-pubs" (x int primary key, y int, "column-1" int);
CREATE TABLE "publication-1"."test-pubs" (x int primary key, y int, "column-1" int);

-- various operations on a publication with only local tables
CREATE PUBLICATION pubtables_orig FOR TABLE test, "test-pubs", "publication-1"."test-pubs" WITH (publish = 'insert, truncate');
ALTER PUBLICATION pubtables_orig DROP TABLE test;
ALTER PUBLICATION pubtables_orig ADD TABLE test;

-- publication will be empty on worker nodes, since all tables are local
SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- distribute a table and create a tenant schema, creating a mixed publication
SELECT create_distributed_table('test','x', colocate_with := 'none');
SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA citus_schema_1;
CREATE TABLE citus_schema_1.test (x int primary key, y int, "column-1" int, doc xml);
SET citus.enable_schema_based_sharding TO OFF;
ALTER PUBLICATION pubtables_orig ADD TABLE citus_schema_1.test;

-- some generic operations
ALTER PUBLICATION pubtables_orig RENAME TO pubtables;
ALTER PUBLICATION pubtables SET (publish = 'insert, update, delete');
ALTER PUBLICATION pubtables OWNER TO postgres;
ALTER PUBLICATION pubtables SET (publish = 'inert, update, delete');
ALTER PUBLICATION pubtables ADD TABLE notexist;

-- operations with a distributed table
ALTER PUBLICATION pubtables DROP TABLE test;
ALTER PUBLICATION pubtables ADD TABLE test;
ALTER PUBLICATION pubtables SET TABLE test, "test-pubs", "publication-1"."test-pubs", citus_schema_1.test;

-- operations with a tenant schema table
ALTER PUBLICATION pubtables DROP TABLE citus_schema_1.test;
ALTER PUBLICATION pubtables ADD TABLE citus_schema_1.test;
ALTER PUBLICATION pubtables SET TABLE test, "test-pubs", "publication-1"."test-pubs", citus_schema_1.test;

-- operations with a local table in a mixed publication
ALTER PUBLICATION pubtables DROP TABLE "test-pubs";
ALTER PUBLICATION pubtables ADD TABLE "test-pubs";

SELECT create_distributed_table('"test-pubs"', 'x');

-- test and test-pubs will show up in worker nodes
SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- operations with a strangely named distributed table in a mixed publication
ALTER PUBLICATION pubtables DROP TABLE "test-pubs";
ALTER PUBLICATION pubtables ADD TABLE "test-pubs";

-- create a publication with distributed and local tables
DROP PUBLICATION pubtables;
CREATE PUBLICATION pubtables FOR TABLE test, "test-pubs", "publication-1"."test-pubs", citus_schema_1.test;

-- change distributed tables
SELECT alter_distributed_table('test', shard_count := 5, cascade_to_colocated := true);
SELECT undistribute_table('test');
SELECT citus_add_local_table_to_metadata('test');
SELECT create_distributed_table_concurrently('test', 'x');
SELECT undistribute_table('"test-pubs"');
SELECT create_reference_table('"test-pubs"');

-- publications are unchanged despite various tranformations
SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- partitioned table
CREATE TABLE testpub_partitioned (a int, b text, c text) PARTITION BY RANGE (a);
CREATE TABLE testpub_partitioned_0 PARTITION OF testpub_partitioned FOR VALUES FROM (1) TO (10);
ALTER TABLE testpub_partitioned_0 ADD PRIMARY KEY (a);
ALTER TABLE testpub_partitioned_0 REPLICA IDENTITY USING INDEX testpub_partitioned_0_pkey;
CREATE TABLE testpub_partitioned_1 PARTITION OF testpub_partitioned FOR VALUES FROM (11) TO (20);
ALTER TABLE testpub_partitioned_1 ADD PRIMARY KEY (a);
ALTER TABLE testpub_partitioned_1 REPLICA IDENTITY USING INDEX testpub_partitioned_1_pkey;
CREATE PUBLICATION pubpartitioned FOR TABLE testpub_partitioned WITH (publish_via_partition_root = 'true');

SELECT create_distributed_table('testpub_partitioned', 'a');

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubpartitioned%' ORDER BY 1) s$$)
  ORDER BY c) s;

DROP PUBLICATION pubpartitioned;
CREATE PUBLICATION pubpartitioned FOR TABLE testpub_partitioned WITH (publish_via_partition_root = 'true');

-- add a partition
ALTER PUBLICATION pubpartitioned ADD TABLE testpub_partitioned_1;

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLIATION%' AND c LIKE '%pubpartitioned%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- make sure we can sync all the publication metadata
SELECT start_metadata_sync_to_all_nodes();

DROP PUBLICATION pubempty;
DROP PUBLICATION pubtables;
DROP PUBLICATION pubinsertonly;
DROP PUBLICATION "pub-all-insertupdateonly";
DROP PUBLICATION "pub-all";
DROP PUBLICATION pubpartitioned;
DROP PUBLICATION pubnotdistributed;

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
SET client_min_messages TO ERROR;
DROP SCHEMA publication CASCADE;
DROP SCHEMA "publication-1" CASCADE;
DROP SCHEMA citus_schema_1 CASCADE;
\q
\endif

-- recreate a mixed publication
CREATE PUBLICATION pubtables FOR TABLE test, "publication-1"."test-pubs", citus_schema_1.test;

-- operations on an existing distributed table
ALTER PUBLICATION pubtables DROP TABLE test;
ALTER PUBLICATION pubtables ADD TABLE test (y);
ALTER PUBLICATION pubtables SET TABLE test WHERE (doc IS DOCUMENT);
ALTER PUBLICATION pubtables SET TABLE test WHERE (xmlexists('//foo[text() = ''bar'']' PASSING BY VALUE doc));
ALTER PUBLICATION pubtables SET TABLE test WHERE (CASE x WHEN 5 THEN true ELSE false END);

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- operations on an existing tenant schema table
ALTER PUBLICATION pubtables ADD TABLE citus_schema_1.test (y);
ALTER PUBLICATION pubtables DROP TABLE citus_schema_1.test;
ALTER PUBLICATION pubtables SET TABLE citus_schema_1.test WHERE (doc IS DOCUMENT);
ALTER PUBLICATION pubtables SET TABLE citus_schema_1.test WHERE (xmlexists('//foo[text() = ''bar'']' PASSING BY VALUE doc));
ALTER PUBLICATION pubtables SET TABLE citus_schema_1.test WHERE (CASE x WHEN 5 THEN true ELSE false END);

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

ALTER PUBLICATION pubtables SET TABLE test ("column-1", x) WHERE (x > "column-1"), "publication-1"."test-pubs";

-- operations on a local table
ALTER PUBLICATION pubtables DROP TABLE "publication-1"."test-pubs";
ALTER PUBLICATION pubtables ADD TABLE "publication-1"."test-pubs" (y);

-- mixed operations
ALTER PUBLICATION pubtables SET TABLE test, TABLES IN SCHEMA "publication-1", TABLES IN SCHEMA current_schema;
ALTER PUBLICATION pubtables SET TABLE "publication-1"."test-pubs", test ("column-1", x) WHERE (x > "column-1");

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubtables%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- publication with schemas
CREATE PUBLICATION "pub-mix" FOR TABLE test, TABLES IN SCHEMA current_schema, TABLE "publication-1"."test-pubs", TABLES IN SCHEMA "publication-1";

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pub-mix%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- publication on a partitioned table
CREATE PUBLICATION pubpartitioned FOR TABLE testpub_partitioned (a, b) WITH (publish_via_partition_root = 'true');
ALTER PUBLICATION pubpartitioned SET (publish_via_partition_root = 1);

SELECT alter_distributed_table('testpub_partitioned', shard_count := 6, cascade_to_colocated := true);

SELECT DISTINCT c FROM (
  SELECT unnest(result::text[]) c
  FROM run_command_on_workers($$
    SELECT array_agg(c) FROM (SELECT c FROM unnest(activate_node_snapshot()) c WHERE c LIKE '%CREATE PUBLICATION%' AND c LIKE '%pubpartitioned%' ORDER BY 1) s$$)
  ORDER BY c) s;

-- make sure we propagate schema dependencies
CREATE SCHEMA deptest;
CREATE PUBLICATION pubdep FOR TABLES IN SCHEMA deptest;
DROP SCHEMA deptest CASCADE;

-- make sure we can sync all the publication metadata
SELECT start_metadata_sync_to_all_nodes();

DROP PUBLICATION pubdep;
DROP PUBLICATION "pub-mix";
DROP PUBLICATION pubtables;
DROP PUBLICATION pubpartitioned;

SET client_min_messages TO ERROR;
DROP SCHEMA publication CASCADE;
DROP SCHEMA "publication-1" CASCADE;
DROP SCHEMA citus_schema_1 CASCADE;
