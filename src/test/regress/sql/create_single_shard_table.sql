CREATE SCHEMA create_single_shard_table;
SET search_path TO create_single_shard_table;

SET citus.next_shard_id TO 1720000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SELECT 1 FROM citus_remove_node('localhost', :worker_1_port);
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

CREATE TABLE add_node_test(a int, "b" text);

-- add a node before creating the single-shard table
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);

SELECT create_distributed_table('add_node_test', null, colocate_with=>'none', distribution_type=>null);

-- add a node after creating the single-shard table
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- make sure that table is created on the worker nodes added before/after create_distributed_table
SELECT result FROM run_command_on_workers($$
    SELECT COUNT(*)=1 FROM pg_class WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
                                          relname='add_node_test'
$$);

-- and check the metadata tables

SELECT result FROM run_command_on_workers($$
    SELECT (partmethod, partkey, repmodel, autoconverted) FROM pg_dist_partition
    WHERE logicalrelid = 'create_single_shard_table.add_node_test'::regclass
$$);

SELECT result FROM run_command_on_workers($$
    SELECT (shardstorage, shardminvalue, shardmaxvalue) FROM pg_dist_shard
    WHERE logicalrelid = 'create_single_shard_table.add_node_test'::regclass
$$);

SELECT result FROM run_command_on_workers($$
    SELECT COUNT(*)=1 FROM pg_dist_placement
    WHERE shardid = (
        SELECT shardid FROM pg_dist_shard
        WHERE logicalrelid = 'create_single_shard_table.add_node_test'::regclass
    );
$$);

SELECT result FROM run_command_on_workers($$
    SELECT (shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation) FROM pg_dist_colocation
    WHERE colocationid = (
        SELECT colocationid FROM pg_dist_partition
        WHERE logicalrelid = 'create_single_shard_table.add_node_test'::regclass
    );
$$);

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

SET client_min_messages TO NOTICE;

CREATE TABLE invalid_configs_1(a int primary key);
SELECT create_distributed_table('invalid_configs_1', null, shard_count=>2);
SELECT create_distributed_table('invalid_configs_1', null, shard_count=>1);

CREATE TABLE nullkey_c1_t1(a int, b int);
CREATE TABLE nullkey_c1_t2(a int, b int);
CREATE TABLE nullkey_c1_t3(a int, b int);
SELECT create_distributed_table('nullkey_c1_t1', null, colocate_with=>'none');

SELECT colocationid AS nullkey_c1_t1_colocationid FROM pg_dist_partition WHERE logicalrelid = 'create_single_shard_table.nullkey_c1_t1'::regclass \gset

BEGIN;
  DROP TABLE nullkey_c1_t1;
  -- make sure that we delete the colocation group after dropping the last table that belongs to it
  SELECT COUNT(*)=0 FROM pg_dist_colocation WHERE colocationid = :'nullkey_c1_t1_colocationid';
ROLLBACK;

SELECT create_distributed_table('nullkey_c1_t2', null, colocate_with=>'nullkey_c1_t1');
SELECT create_distributed_table('nullkey_c1_t3', null, colocate_with=>'nullkey_c1_t1', distribution_type=>'append');
SELECT create_distributed_table('nullkey_c1_t3', null, colocate_with=>'nullkey_c1_t1');

CREATE TABLE nullkey_c2_t1(a int, b int);
CREATE TABLE nullkey_c2_t2(a int, b int);
CREATE TABLE nullkey_c2_t3(a int, b int);

-- create_distributed_table_concurrently is not yet supported yet
SELECT create_distributed_table_concurrently('nullkey_c2_t1', null);
SELECT create_distributed_table_concurrently('nullkey_c2_t1', null, colocate_with=>'none');

SELECT create_distributed_table('nullkey_c2_t1', null, colocate_with=>'none');
SELECT create_distributed_table('nullkey_c2_t2', null, colocate_with=>'nullkey_c2_t1', distribution_type=>'hash'); -- distribution_type is ignored anyway
SELECT create_distributed_table('nullkey_c2_t3', null, colocate_with=>'nullkey_c2_t2', distribution_type=>null);

-- check the metadata for the colocated tables whose names start with nullkey_c1_

SELECT logicalrelid, partmethod, partkey, repmodel, autoconverted FROM pg_dist_partition
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c1_%'
)
ORDER BY 1;

-- make sure that all those 3 tables belong to same colocation group
SELECT COUNT(*) FROM pg_dist_partition
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c1_%'
)
GROUP BY colocationid;

SELECT logicalrelid, shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c1_%'
)
ORDER BY 1;

-- make sure that all those 3 shards are created on the same node group
SELECT COUNT(*) FROM pg_dist_placement
WHERE shardid IN (
    SELECT shardid FROM pg_dist_shard
    WHERE logicalrelid IN (
        SELECT oid FROM pg_class
        WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
              relname LIKE 'nullkey_c1_%'
    )
)
GROUP BY groupid;

-- check the metadata for the colocated tables whose names start with nullkey_c2_

SELECT logicalrelid, partmethod, partkey, repmodel, autoconverted FROM pg_dist_partition
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c2_%'
)
ORDER BY 1;

-- make sure that all those 3 tables belong to same colocation group
SELECT COUNT(*) FROM pg_dist_partition
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c2_%'
)
GROUP BY colocationid;

SELECT logicalrelid, shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
WHERE logicalrelid IN (
    SELECT oid FROM pg_class
    WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
          relname LIKE 'nullkey_c2_%'
)
ORDER BY 1;

-- make sure that all those 3 shards created on the same node group
SELECT COUNT(*) FROM pg_dist_placement
WHERE shardid IN (
    SELECT shardid FROM pg_dist_shard
    WHERE logicalrelid IN (
        SELECT oid FROM pg_class
        WHERE relnamespace = 'create_single_shard_table'::regnamespace AND
              relname LIKE 'nullkey_c2_%'
    )
)
GROUP BY groupid;

-- Make sure that the colocated tables whose names start with nullkey_c1_
-- belong to a different colocation group than the ones whose names start
-- with nullkey_c2_.
--
-- It's ok to only compare nullkey_c1_t1 and nullkey_c2_t1 because we already
-- verified that null_dist_key.nullkey_c1_t1 is colocated with the other two
-- and null_dist_key.nullkey_c2_t1 is colocated with the other two.
SELECT
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_single_shard_table.nullkey_c1_t1'::regclass
)
!=
(
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_single_shard_table.nullkey_c2_t1'::regclass
);

-- Since we determine node for the placement based on the module of colocation id,
-- we don't expect those two colocation groups to get assigned to same node.
SELECT
(
    SELECT groupid FROM pg_dist_placement
    WHERE shardid = (
        SELECT shardid FROM pg_dist_shard
        WHERE logicalrelid = 'create_single_shard_table.nullkey_c1_t1'::regclass
    )
)
!=
(
    SELECT groupid FROM pg_dist_placement
    WHERE shardid = (
        SELECT shardid FROM pg_dist_shard
        WHERE logicalrelid = 'create_single_shard_table.nullkey_c2_t1'::regclass
    )
);

-- It's ok to only check nullkey_c1_t1 and nullkey_c2_t1 because we already
-- verified that null_dist_key.nullkey_c1_t1 is colocated with the other two
-- and null_dist_key.nullkey_c2_t1 is colocated with the other two.
SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid = (
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_single_shard_table.nullkey_c1_t1'::regclass
);

SELECT shardcount, replicationfactor, distributioncolumntype, distributioncolumncollation FROM pg_dist_colocation
WHERE colocationid = (
    SELECT colocationid FROM pg_dist_partition
    WHERE logicalrelid = 'create_single_shard_table.nullkey_c2_t1'::regclass
);

CREATE TABLE round_robin_test_c1(a int, b int);
SELECT create_distributed_table('round_robin_test_c1', null, colocate_with=>'none', distribution_type=>null);

\c - - - :master_port
SET search_path TO create_single_shard_table;
SET citus.next_shard_id TO 1730000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

CREATE TABLE round_robin_test_c2(a int, b int);
SELECT create_distributed_table('round_robin_test_c2', null, colocate_with=>'none', distribution_type=>null);

-- Since we determine node for the placement based on the module of colocation id,
-- we don't expect those two colocation groups to get assigned to same node even
-- after reconnecting to the coordinator.
SELECT
(
    SELECT groupid FROM pg_dist_placement
    WHERE shardid = (
        SELECT shardid FROM pg_dist_shard
        WHERE logicalrelid = 'create_single_shard_table.round_robin_test_c1'::regclass
    )
)
!=
(
    SELECT groupid FROM pg_dist_placement
    WHERE shardid = (
        SELECT shardid FROM pg_dist_shard
        WHERE logicalrelid = 'create_single_shard_table.round_robin_test_c2'::regclass
    )
);

CREATE TABLE distributed_table(a int, b int);

-- cannot colocate a sharded table with single-shard table
SELECT create_distributed_table('distributed_table', 'a', colocate_with=>'nullkey_c1_t1');

CREATE TABLE reference_table(a int, b int);
CREATE TABLE local(a int, b int);
SELECT create_reference_table('reference_table');
SELECT create_distributed_table('distributed_table', 'a');

-- cannot colocate single-shard tables with other table types
CREATE TABLE cannot_colocate_with_other_types (a int, b int);
SELECT create_distributed_table('cannot_colocate_with_other_types', null, colocate_with=>'reference_table');
SELECT create_distributed_table('cannot_colocate_with_other_types', null, colocate_with=>'distributed_table');
SELECT create_distributed_table('cannot_colocate_with_other_types', null, colocate_with=>'local'); -- postgres local

SELECT citus_add_local_table_to_metadata('local');

-- cannot colocate single-shard tables with citus local tables
SELECT create_distributed_table('cannot_colocate_with_other_types', null, colocate_with=>'local'); -- citus local

SET client_min_messages TO WARNING;

-- can't create such a distributed table from another Citus table, except Citus local tables
SELECT create_distributed_table('reference_table', null, colocate_with=>'none');
SELECT create_distributed_table('distributed_table', null, colocate_with=>'none');
SELECT create_distributed_table('local', null, colocate_with=>'none');

BEGIN;
  -- creating a single-shard table from a temporary table is not supported
  CREATE TEMPORARY TABLE temp_table (a int);
  SELECT create_distributed_table('temp_table', null, colocate_with=>'none', distribution_type=>null);
ROLLBACK;

-- creating a single-shard table from a catalog table is not supported
SELECT create_distributed_table('pg_catalog.pg_index', NULL, distribution_type=>null);

-- creating a single-shard table from an unlogged table is supported
CREATE UNLOGGED TABLE unlogged_table (a int);
SELECT create_distributed_table('unlogged_table', null, colocate_with=>'none', distribution_type=>null);

-- creating a single-shard table from a foreign table is not supported
CREATE FOREIGN TABLE foreign_table (
  id bigint not null,
  full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true', table_name 'foreign_table');
SELECT create_distributed_table('foreign_table', null, colocate_with=>'none', distribution_type=>null);

-- create a single-shard table that has no tuples
CREATE TABLE null_dist_key_table_1 (a int primary key);
SELECT create_distributed_table('null_dist_key_table_1', null, colocate_with=>'none');

-- create a single-shard table that has some tuples
CREATE TABLE null_dist_key_table_2(a int primary key);
INSERT INTO null_dist_key_table_2 VALUES(1);
SELECT create_distributed_table('null_dist_key_table_2', null, colocate_with=>'none');

SELECT * FROM null_dist_key_table_2 ORDER BY a;

DROP TABLE null_dist_key_table_1, null_dist_key_table_2;

-- create indexes before creating the single-shard tables

-- .. for an initially empty table
CREATE TABLE null_dist_key_table_1(a int, b int);
CREATE STATISTICS s1 (dependencies) ON a, b FROM null_dist_key_table_1;
CREATE INDEX null_dist_key_table_1_idx ON null_dist_key_table_1(a);
SELECT create_distributed_table('null_dist_key_table_1', null, colocate_with=>'none');
CREATE STATISTICS s2 (dependencies) ON a, b FROM null_dist_key_table_1;

-- .. and for another table having data in it before creating single-shard table
CREATE TABLE null_dist_key_table_2(a int);
INSERT INTO null_dist_key_table_2 VALUES(1);
CREATE INDEX null_dist_key_table_2_idx ON null_dist_key_table_2(a);
SELECT create_distributed_table('null_dist_key_table_2', null, colocate_with=>'none');

-- test create index concurrently, then reindex
CREATE INDEX CONCURRENTLY ind_conc ON null_dist_key_table_2(a);
REINDEX INDEX ind_conc;
REINDEX INDEX CONCURRENTLY ind_conc;
DROP INDEX ind_conc;

SELECT * FROM null_dist_key_table_2 ORDER BY a;

-- show that we do not support inheritance relationships
CREATE TABLE parent_table (a int, b text);
CREATE TABLE child_table () INHERITS (parent_table);

-- both of below should error out
SELECT create_distributed_table('parent_table', null, colocate_with=>'none');
SELECT create_distributed_table('child_table', null, colocate_with=>'none');

-- show that we support policies
BEGIN;
  CREATE TABLE null_dist_key_table_3 (table_user text);

  ALTER TABLE null_dist_key_table_3 ENABLE ROW LEVEL SECURITY;

  CREATE ROLE table_users;
  CREATE POLICY table_policy ON null_dist_key_table_3 TO table_users
      USING (table_user = current_user);

  GRANT ALL ON TABLE null_dist_key_table_3 TO table_users;
  ALTER TABLE null_dist_key_table_3 OWNER TO table_users;

  SELECT create_distributed_table('null_dist_key_table_3', null, colocate_with=>'none');

  REVOKE ALL ON TABLE null_dist_key_table_3 FROM table_users;
  ALTER TABLE null_dist_key_table_3 OWNER TO postgres;
  GRANT ALL ON TABLE null_dist_key_table_3 TO table_users;
ROLLBACK;

ALTER STATISTICS s2 SET STATISTICS 46;
ALTER TABLE null_dist_key_table_1 SET SCHEMA public;
DROP STATISTICS s1, s2;

-- drop them for next tests
DROP TABLE public.null_dist_key_table_1, null_dist_key_table_2, distributed_table;

-- tests for object names that should be escaped properly

CREATE SCHEMA "NULL_!_dist_key";

CREATE TABLE "NULL_!_dist_key"."my_TABLE.1!?!"(id int, "Second_Id" int);
SELECT create_distributed_table('"NULL_!_dist_key"."my_TABLE.1!?!"', null, colocate_with=>'none');

-- drop the table before creating it when the search path is set
SET search_path to "NULL_!_dist_key" ;
DROP TABLE "my_TABLE.1!?!";

CREATE TYPE int_jsonb_type AS (key int, value jsonb);
CREATE DOMAIN age_with_default AS int CHECK (value >= 0) DEFAULT 0;
CREATE TYPE yes_no_enum AS ENUM ('yes', 'no');
CREATE EXTENSION btree_gist;

CREATE SEQUENCE my_seq_1 START WITH 10;

CREATE TABLE "Table?!.1Table"(
  id int PRIMARY KEY,
  "Second_Id" int,
  "local_Type" int_jsonb_type,
  "jsondata" jsonb NOT NULL,
  name text,
  price numeric CHECK (price > 0),
  age_with_default_col age_with_default,
  yes_no_enum_col yes_no_enum,
  seq_col_1 bigserial,
  seq_col_2 int DEFAULT nextval('my_seq_1'),
  generated_column int GENERATED ALWAYS AS (seq_col_1 * seq_col_2 + 4) STORED,
  UNIQUE (id, price),
  EXCLUDE USING GIST (name WITH =));

-- create some objects before create_distributed_table
CREATE INDEX "my!Index1" ON "Table?!.1Table"(id) WITH ( fillfactor = 80 ) WHERE id > 10;
CREATE INDEX text_index ON "Table?!.1Table"(name);
CREATE UNIQUE INDEX uniqueIndex ON "Table?!.1Table" (id);
CREATE STATISTICS stats_1 ON id, price FROM "Table?!.1Table";

CREATE TEXT SEARCH CONFIGURATION text_search_cfg (parser = default);
CREATE INDEX text_search_index ON "Table?!.1Table"
USING gin (to_tsvector('text_search_cfg'::regconfig, (COALESCE(name, ''::character varying))::text));

-- ingest some data before create_distributed_table
INSERT INTO "Table?!.1Table" VALUES (1, 1, (1, row_to_json(row(1,1)))::int_jsonb_type, row_to_json(row(1,1), true)),
                                    (2, 1, (2, row_to_json(row(2,2)))::int_jsonb_type, row_to_json(row(2,2), 'false'));

-- create a replica identity before create_distributed_table
ALTER TABLE "Table?!.1Table" REPLICA IDENTITY USING INDEX uniqueIndex;

SELECT create_distributed_table('"Table?!.1Table"', null, colocate_with=>'none');

INSERT INTO "Table?!.1Table" VALUES (10, 15, (150, row_to_json(row(4,8)))::int_jsonb_type, '{}', 'text_1', 10, 27, 'yes', 60, 70);
INSERT INTO "Table?!.1Table" VALUES (5, 5, (5, row_to_json(row(5,5)))::int_jsonb_type, row_to_json(row(5,5), true));

-- tuples that are supposed to violate different data type / check constraints
INSERT INTO "Table?!.1Table"(id, jsondata, name) VALUES (101, '{"a": 1}', 'text_1');
INSERT INTO "Table?!.1Table"(id, jsondata, price) VALUES (101, '{"a": 1}', -1);
INSERT INTO "Table?!.1Table"(id, jsondata, age_with_default_col) VALUES (101, '{"a": 1}', -1);
INSERT INTO "Table?!.1Table"(id, jsondata, yes_no_enum_col) VALUES (101, '{"a": 1}', 'what?');

SELECT * FROM "Table?!.1Table" ORDER BY id;

SET search_path TO create_single_shard_table;

-- create a partitioned table with some columns that
-- are going to be dropped within the tests
CREATE TABLE sensors(
    col_to_drop_1 text,
    measureid integer,
    eventdatetime date,
    measure_data jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data))
PARTITION BY RANGE(eventdatetime);

-- drop column even before attaching any partitions
ALTER TABLE sensors DROP COLUMN col_to_drop_1;

CREATE TABLE sensors_2000 PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');

-- cannot distribute child table without distributing the parent
SELECT create_distributed_table('sensors_2000', NULL, distribution_type=>null);

SELECT create_distributed_table('sensors', NULL, distribution_type=>null);

-- verify we can create new partitions after distributing the parent table
CREATE TABLE sensors_2001 PARTITION OF sensors FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');

-- verify we can attach to a single-shard table
CREATE TABLE sensors_2002 (measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data));
ALTER TABLE sensors ATTACH PARTITION sensors_2002 FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

-- verify we can detach from a single-shard table
ALTER TABLE sensors DETACH PARTITION sensors_2001;

-- error out when attaching a noncolocated partition
CREATE TABLE sensors_2003 (measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data));
SELECT create_distributed_table('sensors_2003', NULL, distribution_type=>null, colocate_with=>'none');
ALTER TABLE sensors ATTACH PARTITION sensors_2003 FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');
DROP TABLE sensors_2003;

-- verify we can attach after distributing, if the parent and partition are colocated
CREATE TABLE sensors_2004 (measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data));
SELECT create_distributed_table('sensors_2004', NULL, distribution_type=>null, colocate_with=>'sensors');
ALTER TABLE sensors ATTACH PARTITION sensors_2004 FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');

-- verify we can attach a citus local table
CREATE TABLE sensors_2005 (measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data));
SELECT citus_add_local_table_to_metadata('sensors_2005');
ALTER TABLE sensors ATTACH PARTITION sensors_2005 FOR VALUES FROM ('2005-01-01') TO ('2006-01-01');

-- check metadata
-- check all partitions and the parent on pg_dist_partition
SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::text IN ('sensors', 'sensors_2000', 'sensors_2001', 'sensors_2002', 'sensors_2004', 'sensors_2005') ORDER BY logicalrelid::text;
-- verify they are all colocated
SELECT COUNT(DISTINCT(colocationid)) FROM pg_dist_partition WHERE logicalrelid::text IN ('sensors', 'sensors_2000', 'sensors_2001', 'sensors_2002', 'sensors_2004', 'sensors_2005');
-- verify all partitions are placed on the same node
SELECT COUNT(DISTINCT(groupid)) FROM pg_dist_placement WHERE shardid IN
    (SELECT shardid FROM pg_dist_shard WHERE logicalrelid::text IN ('sensors', 'sensors_2000', 'sensors_2001', 'sensors_2002', 'sensors_2004', 'sensors_2005'));

-- verify the shard of sensors_2000 is attached to the parent shard, on the worker node
SELECT COUNT(*) FROM run_command_on_workers($$
    SELECT relpartbound FROM pg_class WHERE relname LIKE 'sensors_2000_1______';$$)
    WHERE length(result) > 0;

-- verify the shard of sensors_2001 is detached from the parent shard, on the worker node
SELECT COUNT(*) FROM run_command_on_workers($$
    SELECT relpartbound FROM pg_class WHERE relname LIKE 'sensors_2001_1______';$$)
    WHERE length(result) > 0;

-- verify the shard of sensors_2002 is attached to the parent shard, on the worker node
SELECT COUNT(*) FROM run_command_on_workers($$
    SELECT relpartbound FROM pg_class WHERE relname LIKE 'sensors_2002_1______';$$)
    WHERE length(result) > 0;

-- create a partitioned citus local table and verify we error out when attaching a partition with single-shard
CREATE TABLE partitioned_citus_local_tbl(
    measureid integer,
    eventdatetime date,
    measure_data jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data))
PARTITION BY RANGE(eventdatetime);
SELECT citus_add_local_table_to_metadata('partitioned_citus_local_tbl');
CREATE TABLE partition_with_null_key (measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data));
SELECT create_distributed_table('partition_with_null_key', NULL, distribution_type=>null);
ALTER TABLE partitioned_citus_local_tbl ATTACH PARTITION partition_with_null_key FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');

-- test partitioned tables + indexes with long names
CREATE TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(
  id int PRIMARY KEY,
  "TeNANt_Id" int,
  "jsondata" jsonb NOT NULL,
  name text,
  price numeric CHECK (price > 0),
  serial_data bigserial, UNIQUE (id, price))
  PARTITION BY LIST(id);

CREATE TABLE "NULL_!_dist_key"."partition1_nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    PARTITION OF "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    FOR VALUES IN (1);
CREATE TABLE "NULL_!_dist_key"."partition2_nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    PARTITION OF "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    FOR VALUES IN (2);
CREATE TABLE "NULL_!_dist_key"."partition100_nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    PARTITION OF "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    FOR VALUES IN (100);

-- create some objects before create_distributed_table
CREATE INDEX "my!Index1New" ON "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) WITH ( fillfactor = 80 ) WHERE  id > 10;
CREATE UNIQUE INDEX uniqueIndexNew ON "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" (id);

-- ingest some data before create_distributed_table
set client_min_messages to ERROR;
INSERT INTO "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (1, 1, row_to_json(row(1,1), true)),
                                     (2, 1, row_to_json(row(2,2), 'false'));
reset client_min_messages;
-- create a replica identity before create_distributed_table
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" REPLICA IDENTITY USING INDEX uniqueIndexNew;

-- test triggers
SET client_min_messages TO ERROR;
CREATE FUNCTION insert_id_100() RETURNS trigger AS $insert_100$
BEGIN
    INSERT INTO "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (100, 1, row_to_json(row(1,1), true));
    RETURN NEW;
END;
$insert_100$ LANGUAGE plpgsql;

CREATE TABLE null_key_table_with_trigger(a INT);
SELECT create_distributed_table('null_key_table_with_trigger', null);
-- try to add a trigger after distributing the table, fails
CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON null_key_table_with_trigger
    FOR EACH STATEMENT EXECUTE FUNCTION insert_id_100();

-- now try to distribute a table that already has a trigger on it
CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    FOR EACH STATEMENT EXECUTE FUNCTION insert_id_100();

-- error out because of the trigger
SELECT create_distributed_table('"NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"', null);

SET citus.enable_unsafe_triggers TO ON;
RESET client_min_messages;

-- this shouldn't give any syntax errors
SELECT create_distributed_table('"NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"', null);

-- now we can add triggers on distributed tables, because we set the GUC to on
CREATE TRIGGER insert_100_trigger_2
    AFTER UPDATE ON null_key_table_with_trigger
    FOR EACH STATEMENT EXECUTE FUNCTION insert_id_100();

SET client_min_messages TO ERROR;
UPDATE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" SET "TeNANt_Id"="TeNANt_Id"+1;
-- we should see one row with id = 100
SELECT COUNT(*) FROM "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" WHERE id = 100;

-- create some objects after create_distributed_table
CREATE INDEX "my!Index2New" ON "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) WITH ( fillfactor = 90 ) WHERE id < 20;
CREATE UNIQUE INDEX uniqueIndex2New ON "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

-- error out for already existing, because of the unique index
INSERT INTO "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (1, 1, row_to_json(row(1,1), true));

-- verify all 4 shard indexes are created on the same node
SELECT result FROM run_command_on_workers($$
    SELECT COUNT(*) FROM pg_indexes WHERE indexname LIKE '%my!Index_New_1%' OR indexname LIKE '%uniqueindex%new_1%';$$)
    ORDER BY nodeport;

-- foreign key to a ref table
CREATE TABLE dummy_reference_table (a INT PRIMARY KEY);
SELECT create_reference_table('dummy_reference_table');
TRUNCATE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789";
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (id) REFERENCES dummy_reference_table(a);
BEGIN; -- try to add the same fkey, reversed
    ALTER TABLE dummy_reference_table
        ADD CONSTRAINT fkey_to_dummy_ref FOREIGN KEY (a) REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);
ROLLBACK;

-- errors out because of foreign key violation
INSERT INTO "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (100, 1, row_to_json(row(1,1), true));

-- now inserts successfully
INSERT INTO dummy_reference_table VALUES (100);
INSERT INTO "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" VALUES (100, 1, row_to_json(row(1,1), true));
DELETE FROM "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" WHERE id = 100;

-- foreign key to a local table, errors out
CREATE TABLE local_table_for_fkey (a INT PRIMARY KEY);
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_local FOREIGN KEY (id) REFERENCES local_table_for_fkey(a);

-- foreign key from a local table
ALTER TABLE local_table_for_fkey
    ADD CONSTRAINT fkey_from_dummy_local FOREIGN KEY (a) REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

SELECT create_distributed_table('local_table_for_fkey', null, colocate_with=>'none');
SELECT create_distributed_table('local_table_for_fkey', null, colocate_with=>'"NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"');

-- foreign key to a citus local table, errors out
CREATE TABLE citus_local_table_for_fkey (a INT PRIMARY KEY);
SELECT citus_add_local_table_to_metadata('citus_local_table_for_fkey');
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_citus_local FOREIGN KEY (id) REFERENCES citus_local_table_for_fkey(a);
-- reversed, still fails
ALTER TABLE citus_local_table_for_fkey
    ADD CONSTRAINT fkey_from_dummy_citus_local FOREIGN KEY (a) REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

-- foreign key to a distributed table, errors out because not colocated
CREATE TABLE dist_table_for_fkey (a INT PRIMARY KEY);
SELECT create_distributed_table('dist_table_for_fkey', 'a');
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_dist FOREIGN KEY (id) REFERENCES dist_table_for_fkey(a);
-- reversed, still fails
ALTER TABLE dist_table_for_fkey
    ADD CONSTRAINT fkey_to_dummy_dist FOREIGN KEY (a) REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id);

-- create a null key distributed table, not colocated with the partitioned table, and then try to create a fkey
CREATE TABLE null_key_dist_not_colocated (a INT PRIMARY KEY);
SELECT create_distributed_table('null_key_dist_not_colocated', null, colocate_with=>'none');
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_dist FOREIGN KEY (id) REFERENCES null_key_dist_not_colocated(a);

-- create a null key distributed table, colocated with the partitioned table, and then create a fkey
CREATE TABLE null_key_dist (a INT PRIMARY KEY);
SELECT create_distributed_table('null_key_dist', null, colocate_with=>'"NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"');
ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"
    ADD CONSTRAINT fkey_to_dummy_dist FOREIGN KEY (id) REFERENCES null_key_dist(a);

-- check supported ON DELETE and ON UPDATE commands
ALTER TABLE null_key_dist ADD CONSTRAINT fkey_add_test_1 FOREIGN KEY(a)
    REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) ON DELETE SET DEFAULT;
ALTER TABLE null_key_dist ADD CONSTRAINT fkey_add_test_2 FOREIGN KEY(a)
    REFERENCES "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789"(id) ON UPDATE CASCADE;
ALTER TABLE null_key_dist ADD CONSTRAINT fkey_add_test_3 FOREIGN KEY(a)
    REFERENCES dummy_reference_table(a) ON DELETE SET DEFAULT;
ALTER TABLE null_key_dist ADD CONSTRAINT fkey_add_test_4 FOREIGN KEY(a)
    REFERENCES dummy_reference_table(a) ON UPDATE CASCADE;

ALTER TABLE null_key_dist DROP CONSTRAINT fkey_add_test_1;
ALTER TABLE null_key_dist DROP CONSTRAINT fkey_add_test_2;
ALTER TABLE null_key_dist DROP CONSTRAINT fkey_add_test_3;

-- mimic a conjoined django command
CREATE FUNCTION drop_fkey_4()
  RETURNS void
  LANGUAGE plpgsql
AS $function$
BEGIN
  EXECUTE $$
    SET CONSTRAINTS fkey_add_test_4 IMMEDIATE;
    ALTER TABLE null_key_dist DROP CONSTRAINT fkey_add_test_4;
  $$;
END
$function$;
SELECT drop_fkey_4();

ALTER TABLE "NULL_!_dist_key"."nullKeyTable.1!?!9012345678901234567890123456789012345678901234567890123456789" DROP CONSTRAINT fkey_to_dummy_dist;

-- create a view that depends on the single-shard table
CREATE VIEW public.v1 AS SELECT * FROM null_key_dist;
SELECT * FROM public.v1;

DELETE FROM null_key_dist;
VACUUM null_key_dist;
TRUNCATE null_key_dist;
DROP TABLE null_key_dist CASCADE;

RESET client_min_messages;

CREATE TABLE multi_level_partitioning_parent(
    measureid integer,
    eventdatetime date,
    measure_data jsonb)
PARTITION BY RANGE(eventdatetime);

CREATE TABLE multi_level_partitioning_level_1(
    measureid integer,
    eventdatetime date,
    measure_data jsonb)
PARTITION BY RANGE(eventdatetime);

ALTER TABLE multi_level_partitioning_parent ATTACH PARTITION multi_level_partitioning_level_1
FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');

CREATE TABLE multi_level_partitioning_level_2 PARTITION OF multi_level_partitioning_level_1
FOR VALUES FROM ('2000-01-01') TO ('2000-06-06');

-- multi-level partitioning is not supported
SELECT create_distributed_table('multi_level_partitioning_parent', NULL, distribution_type=>null);

CREATE FUNCTION normalize_generate_always_as_error(query text) RETURNS void AS $$
BEGIN
        EXECUTE query;
        EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE 'cannot insert into column %' OR
           SQLERRM LIKE 'cannot insert a non-DEFAULT value into column %'
        THEN
            RAISE 'cannot insert a non-DEFAULT value into column';
        ELSE
            RAISE 'unknown error';
        END IF;
END;
$$LANGUAGE plpgsql;

CREATE TABLE identity_test (
    a int GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10),
    b bigint GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 100),
    c bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 1000 INCREMENT BY 1000)
);

SELECT create_distributed_table('identity_test', NULL, distribution_type=>null);

INSERT INTO identity_test (a) VALUES (DEFAULT) RETURNING a;
SELECT result FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.identity_test (a) VALUES (DEFAULT)
$$);

DROP TABLE identity_test;

CREATE TABLE identity_test (
    a bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 10),
    b bigint GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 100),
    c bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 1000 INCREMENT BY 1000)
);

SELECT create_distributed_table('identity_test', NULL, distribution_type=>null);

INSERT INTO identity_test (a) VALUES (5);

SELECT normalize_generate_always_as_error($$INSERT INTO identity_test (b) VALUES (5)$$); -- fails due to missing OVERRIDING SYSTEM VALUE
INSERT INTO identity_test (b) OVERRIDING SYSTEM VALUE VALUES (5);

INSERT INTO identity_test (c) VALUES (5);

SELECT result, success FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.identity_test (a) VALUES (6)
$$);

SELECT result, success FROM run_command_on_workers($$
    SELECT create_single_shard_table.normalize_generate_always_as_error('INSERT INTO create_single_shard_table.identity_test (b) VALUES (1)')
$$);

-- This should fail due to missing OVERRIDING SYSTEM VALUE.
SELECT result, success FROM run_command_on_workers($$
    SELECT create_single_shard_table.normalize_generate_always_as_error('INSERT INTO create_single_shard_table.identity_test (a, b) VALUES (1, 1)')
$$);

SELECT result, success FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.identity_test (a, b) OVERRIDING SYSTEM VALUE VALUES (7, 7)
$$);

SELECT result, success FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.identity_test (c, a) OVERRIDING SYSTEM VALUE VALUES (8, 8)
$$);

-- test foreign keys

CREATE TABLE referenced_table(a int UNIQUE, b int);
CREATE TABLE referencing_table(a int, b int,
    FOREIGN KEY (a) REFERENCES referenced_table(a));

-- to a colocated single-shard table
BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  -- fails
  INSERT INTO referencing_table VALUES (2, 2);
ROLLBACK;

-- to a non-colocated single-shard table
BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');
ROLLBACK;

-- to a sharded table
BEGIN;
  SELECT create_distributed_table('referenced_table', 'a');

  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null);
ROLLBACK;

-- to a reference table
BEGIN;
  SELECT create_reference_table('referenced_table');
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null);

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  -- fails
  INSERT INTO referencing_table VALUES (2, 2);
ROLLBACK;

-- to a citus local table
BEGIN;
  SELECT citus_add_local_table_to_metadata('referenced_table', cascade_via_foreign_keys=>true);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null);
ROLLBACK;

-- to a postgres table
SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null);

-- from a reference table
BEGIN;
  SELECT create_reference_table('referencing_table');
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
ROLLBACK;

BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
  SELECT create_reference_table('referencing_table');
ROLLBACK;

-- from a sharded table
BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
  SELECT create_distributed_table('referencing_table', 'a');
ROLLBACK;

-- from a citus local table
BEGIN;
  SELECT citus_add_local_table_to_metadata('referencing_table', cascade_via_foreign_keys=>true);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
ROLLBACK;

BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
  SELECT citus_add_local_table_to_metadata('referencing_table', cascade_via_foreign_keys=>true);
ROLLBACK;

-- from a postgres table (only useful to preserve legacy behavior)
BEGIN;
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);
ROLLBACK;

-- make sure that we enforce the foreign key constraint when inserting from workers too
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null);
INSERT INTO referenced_table VALUES (1, 1);
-- ok
SELECT result, success FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.referencing_table VALUES (1, 2)
$$);
-- fails
SELECT result, success FROM run_command_on_workers($$
    INSERT INTO create_single_shard_table.referencing_table VALUES (2, 2)
$$);

DROP TABLE referencing_table, referenced_table;

CREATE TABLE self_fkey_test(a int UNIQUE, b int,
    FOREIGN KEY (b) REFERENCES self_fkey_test(a),
    FOREIGN KEY (a) REFERENCES self_fkey_test(a));
SELECT create_distributed_table('self_fkey_test', NULL, distribution_type=>null);

INSERT INTO self_fkey_test VALUES (1, 1); -- ok
INSERT INTO self_fkey_test VALUES (2, 3); -- fails

-- similar foreign key tests but this time create the referencing table later on

-- referencing table is a single-shard table

-- to a colocated single-shard table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  -- fails
  INSERT INTO referencing_table VALUES (2, 2);
ROLLBACK;

BEGIN;
  CREATE TABLE referenced_table(a int, b int, UNIQUE(b, a));
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a, b) REFERENCES referenced_table(b, a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');

  INSERT INTO referenced_table VALUES (1, 2);
  INSERT INTO referencing_table VALUES (2, 1);

  -- fails
  INSERT INTO referencing_table VALUES (1, 2);
ROLLBACK;

BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a) ON UPDATE SET NULL);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  UPDATE referenced_table SET a = 5;
  SELECT * FROM referencing_table;
ROLLBACK;

BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a serial, b int, FOREIGN KEY (a) REFERENCES referenced_table(a) ON UPDATE SET DEFAULT);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');
ROLLBACK;

BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a serial, b int);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'referenced_table');
  ALTER TABLE referencing_table ADD CONSTRAINT fkey_to_dummy_ref_on_update FOREIGN KEY (a) REFERENCES referenced_table(a) ON UPDATE SET DEFAULT;
ROLLBACK;

-- to a non-colocated single-shard table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null);

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');
ROLLBACK;

-- to a sharded table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', 'a');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');
ROLLBACK;

-- to a reference table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_reference_table('referenced_table');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  -- fails
  INSERT INTO referencing_table VALUES (2, 2);
ROLLBACK;

BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_reference_table('referenced_table');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a) ON DELETE CASCADE);
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');

  INSERT INTO referenced_table VALUES (1, 1);
  INSERT INTO referencing_table VALUES (1, 2);

  DELETE FROM referenced_table CASCADE;
  SELECT * FROM referencing_table;
ROLLBACK;

-- to a citus local table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT citus_add_local_table_to_metadata('referenced_table');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');
ROLLBACK;

-- to a postgres table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', NULL, distribution_type=>null, colocate_with=>'none');
ROLLBACK;

-- referenced table is a single-shard table

-- from a sharded table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null, colocate_with=>'none');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_distributed_table('referencing_table', 'a');
ROLLBACK;

-- from a reference table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null, colocate_with=>'none');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT create_reference_table('referencing_table');
ROLLBACK;

-- from a citus local table
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null, colocate_with=>'none');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
  SELECT citus_add_local_table_to_metadata('referencing_table', cascade_via_foreign_keys=>true);
ROLLBACK;

-- from a postgres table (only useful to preserve legacy behavior)
BEGIN;
  CREATE TABLE referenced_table(a int UNIQUE, b int);
  SELECT create_distributed_table('referenced_table', NULL, distribution_type=>null, colocate_with=>'none');

  CREATE TABLE referencing_table(a int, b int, FOREIGN KEY (a) REFERENCES referenced_table(a));
ROLLBACK;

-- Test whether we switch to sequential execution to enforce foreign
-- key restrictions.

CREATE TABLE referenced_table(id int PRIMARY KEY, value_1 int);
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table(id int PRIMARY KEY, value_1 int, CONSTRAINT fkey FOREIGN KEY(value_1) REFERENCES referenced_table(id) ON UPDATE CASCADE);
SELECT create_distributed_table('referencing_table', null, colocate_with=>'none', distribution_type=>null);

SET client_min_messages TO DEBUG1;

BEGIN;
	-- Switches to sequential execution because referenced_table is a reference table
	-- and referenced by a single-shard table.
    --
    -- Given that we cannot do parallel access on a single-shard table, this is not useful.
    -- However, this is already what we're doing for, e.g., a foreign key from a
    -- reference table to another reference table.
	TRUNCATE referenced_table CASCADE;
	SELECT COUNT(*) FROM referencing_table;
COMMIT;

BEGIN;
	SELECT COUNT(*) FROM referencing_table;
	-- Doesn't fail because the SELECT didn't perform parallel execution.
	TRUNCATE referenced_table CASCADE;
COMMIT;

BEGIN;
	UPDATE referencing_table SET value_1 = 15;
	-- Doesn't fail because the UPDATE didn't perform parallel execution.
	TRUNCATE referenced_table CASCADE;
COMMIT;

BEGIN;
	SELECT COUNT(*) FROM referenced_table;
	-- doesn't switch to sequential execution
	ALTER TABLE referencing_table ADD COLUMN X INT;
ROLLBACK;

BEGIN;
	-- Switches to sequential execution because referenced_table is a reference table
	-- and referenced by a single-shard table.
    --
    -- Given that we cannot do parallel access on a single-shard table, this is not useful.
    -- However, this is already what we're doing for, e.g., a foreign key from a
    -- reference table to another reference table.
	UPDATE referenced_table SET id = 101 WHERE id = 99;
	UPDATE referencing_table SET value_1 = 15;
ROLLBACK;

BEGIN;
	UPDATE referencing_table SET value_1 = 15;
    -- Doesn't fail because prior UPDATE didn't perform parallel execution.
    UPDATE referenced_table SET id = 101 WHERE id = 99;
ROLLBACK;

SET client_min_messages TO WARNING;

DROP TABLE referenced_table, referencing_table;

-- Test whether we unnecessarily switch to sequential execution
-- when the referenced relation is a single-shard table.

CREATE TABLE referenced_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('referenced_table', null, colocate_with=>'none', distribution_type=>null);

CREATE TABLE referencing_table(id int PRIMARY KEY, value_1 int, CONSTRAINT fkey FOREIGN KEY(value_1) REFERENCES referenced_table(id) ON UPDATE CASCADE);
SELECT create_distributed_table('referencing_table', null, colocate_with=>'referenced_table', distribution_type=>null);

SET client_min_messages TO DEBUG1;

BEGIN;
	SELECT COUNT(*) FROM referenced_table;
	-- Doesn't switch to sequential execution because the referenced_table is
	-- a single-shard table.
	ALTER TABLE referencing_table ADD COLUMN X INT;
ROLLBACK;

BEGIN;
	-- Doesn't switch to sequential execution because the referenced_table is
	-- a single-shard table.
	TRUNCATE referenced_table CASCADE;
	SELECT COUNT(*) FROM referencing_table;
COMMIT;

SET client_min_messages TO WARNING;

CREATE FUNCTION increment_value() RETURNS trigger AS $increment_value$
BEGIN
    NEW.value := NEW.value+1;
    RETURN NEW;
END;
$increment_value$ LANGUAGE plpgsql;

CREATE TABLE trigger_table_1 (value int);

CREATE TRIGGER trigger_1
BEFORE INSERT ON trigger_table_1
FOR EACH ROW EXECUTE FUNCTION increment_value();

SELECT create_distributed_table('trigger_table_1', NULL, distribution_type=>null);

INSERT INTO trigger_table_1 VALUES(1), (2);
SELECT * FROM trigger_table_1 ORDER BY value;

CREATE FUNCTION insert_some() RETURNS trigger AS $insert_some$
BEGIN
    RAISE NOTICE 'inserted some rows';
    RETURN NEW;
END;
$insert_some$ LANGUAGE plpgsql;

CREATE TABLE trigger_table_2 (value int);

CREATE TRIGGER trigger_2
AFTER INSERT ON trigger_table_2
FOR EACH STATEMENT EXECUTE FUNCTION insert_some();

ALTER TABLE trigger_table_2 DISABLE TRIGGER trigger_2;

SELECT create_distributed_table('trigger_table_2', NULL, distribution_type=>null);

SET client_min_messages TO NOTICE;
INSERT INTO trigger_table_2 VALUES(3), (4);
SET client_min_messages TO WARNING;

SELECT * FROM trigger_table_2 ORDER BY value;

CREATE FUNCTION combine_old_new_val() RETURNS trigger AS $combine_old_new_val$
BEGIN
    NEW.value = NEW.value * 10 + OLD.value;
    RETURN NEW;
END;
$combine_old_new_val$ LANGUAGE plpgsql;

CREATE FUNCTION notice_truncate() RETURNS trigger AS $notice_truncate$
BEGIN
    RAISE NOTICE 'notice_truncate()';
    RETURN NEW;
END;
$notice_truncate$ LANGUAGE plpgsql;

CREATE TABLE trigger_table_3 (value int);

CREATE TRIGGER trigger_3
BEFORE UPDATE ON trigger_table_3
FOR EACH ROW EXECUTE FUNCTION combine_old_new_val();

CREATE TRIGGER trigger_4
AFTER TRUNCATE ON trigger_table_3
FOR EACH STATEMENT EXECUTE FUNCTION notice_truncate();

INSERT INTO trigger_table_3 VALUES(3), (4);

SELECT create_distributed_table('trigger_table_3', NULL, distribution_type=>null);

UPDATE trigger_table_3 SET value = 5;
SELECT * FROM trigger_table_3 ORDER BY value;

SET client_min_messages TO NOTICE;
TRUNCATE trigger_table_3;
SET client_min_messages TO WARNING;

-- test rename, disable and drop trigger
ALTER TRIGGER trigger_4 ON trigger_table_3 RENAME TO trigger_new_name;
ALTER TABLE trigger_table_3 DISABLE TRIGGER ALL;
DROP TRIGGER trigger_new_name ON trigger_table_3;
-- enable the remaining triggers
ALTER TABLE trigger_table_3 ENABLE TRIGGER ALL;

-- try a few simple queries at least to make sure that we don't crash
BEGIN;
  INSERT INTO nullkey_c1_t1 SELECT * FROM nullkey_c2_t1;
ROLLBACK;

DROP TRIGGER IF EXISTS trigger_1 ON trigger_table_1;
DROP TRIGGER trigger_2 ON trigger_table_2 CASCADE;
DROP TRIGGER trigger_3 ON trigger_table_3 RESTRICT;

-- cleanup at exit
SET client_min_messages TO ERROR;
DROP SCHEMA create_single_shard_table, "NULL_!_dist_key" CASCADE;
