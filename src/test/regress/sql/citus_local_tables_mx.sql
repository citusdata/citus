\set VERBOSITY terse

SET citus.next_shard_id TO 1508000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_tables_mx;
SET search_path TO citus_local_tables_mx;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

--------------
-- triggers --
--------------

CREATE TABLE citus_local_table (value int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

-- first stop metadata sync to worker_1
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

CREATE FUNCTION dummy_function() RETURNS trigger AS $dummy_function$
BEGIN
    RAISE EXCEPTION 'a trigger that throws this exception';
END;
$dummy_function$ LANGUAGE plpgsql;

CREATE TRIGGER dummy_function_trigger
BEFORE UPDATE OF value ON citus_local_table
FOR EACH ROW EXECUTE FUNCTION dummy_function();

-- Show that we can activate node successfully. That means, we create
-- the function that trigger needs in mx workers too.
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);

-- show that the trigger is not allowed to depend(explicitly) on an extension.
CREATE EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table DEPENDS ON EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table RENAME TO renamed_trigger;
ALTER TABLE citus_local_table DISABLE TRIGGER ALL;
-- show that update trigger mx relation is renamed and disabled.
-- both workers should should print 1.
SELECT run_command_on_workers(
$$
SELECT COUNT(*) FROM pg_trigger
WHERE pg_trigger.tgrelid='citus_local_tables_mx.citus_local_table'::regclass AND
      pg_trigger.tgname='renamed_trigger' AND
      pg_trigger.tgenabled='D'
$$);

CREATE FUNCTION another_dummy_function() RETURNS trigger AS $another_dummy_function$
BEGIN
    RAISE EXCEPTION 'another trigger that throws another exception';
END;
$another_dummy_function$ LANGUAGE plpgsql;
-- Show that we can create the trigger successfully. That means, we create
-- the function that trigger needs in mx worker too when processing CREATE
-- TRIGGER commands.
CREATE TRIGGER another_dummy_function_trigger
AFTER TRUNCATE ON citus_local_table
FOR EACH STATEMENT EXECUTE FUNCTION another_dummy_function();

-- create some test tables before next three sections
-- and define some foreign keys between them

CREATE TABLE citus_local_table_1(l1 int);
SELECT citus_add_local_table_to_metadata('citus_local_table_1');
CREATE TABLE reference_table_1(r1 int primary key);
SELECT create_reference_table('reference_table_1');

ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table_1(r1) ON DELETE CASCADE;

CREATE TABLE citus_local_table_2(l1 int primary key);
SELECT citus_add_local_table_to_metadata('citus_local_table_2');
CREATE TABLE reference_table_2(r1 int);
SELECT create_reference_table('reference_table_2');

ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table_2(l1) ON DELETE RESTRICT;

CREATE TABLE citus_local_table_3(l1 int);
SELECT citus_add_local_table_to_metadata('citus_local_table_3');
CREATE TABLE citus_local_table_4(l1 int primary key);
SELECT citus_add_local_table_to_metadata('citus_local_table_4');

ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_local_to_local FOREIGN KEY(l1) REFERENCES citus_local_table_4(l1) ON UPDATE SET NULL;

-- check stats creation
CREATE TABLE citus_local_table_stats(a int, b int);
CREATE STATISTICS stx1 ON a, b FROM citus_local_table_stats;
SELECT citus_add_local_table_to_metadata('citus_local_table_stats');
CREATE STATISTICS stx2 ON a, b FROM citus_local_table_stats;
CREATE STATISTICS stx3 ON a, b FROM citus_local_table_stats;
CREATE STATISTICS stx4 ON a, b FROM citus_local_table_stats;
ALTER STATISTICS stx3 RENAME TO stx5;
DROP STATISTICS stx4;

SELECT stxname FROM pg_statistic_ext ORDER BY stxname;

-- and switch to worker1
\c - - - :worker_1_port
SET search_path TO citus_local_tables_mx;

-----------------------------------------------------------
-- foreign key from citus local table to reference table --
-----------------------------------------------------------

-- show that on delete cascade works
INSERT INTO reference_table_1 VALUES (11);
INSERT INTO citus_local_table_1 VALUES (11);
DELETE FROM reference_table_1 WHERE r1=11;
-- should print 0 rows
SELECT * FROM citus_local_table_1 ORDER BY l1;

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO citus_local_table_1 VALUES (2);

-- below should work
INSERT INTO reference_table_1 VALUES (2);
INSERT INTO citus_local_table_1 VALUES (2);

-----------------------------------------------------------
-- foreign key from reference table to citus local table --
-----------------------------------------------------------

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO reference_table_2 VALUES (4);

-- below should work
INSERT INTO citus_local_table_2 VALUES (4);
INSERT INTO reference_table_2 VALUES (4);

-------------------------------------------------------------
-- foreign key from citus local table to citus local table --
-------------------------------------------------------------

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO citus_local_table_3 VALUES (3);

-- below shoud work
INSERT INTO citus_local_table_4 VALUES (3);
INSERT INTO citus_local_table_3 VALUES (3);

UPDATE citus_local_table_4 SET l1=6 WHERE l1=3;

-- show that it prints only one row with l1=null due to ON UPDATE SET NULL
SELECT * FROM citus_local_table_3;

-- finally show that we do not allow defining foreign key in mx nodes
ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_local_to_local_2 FOREIGN KEY(l1) REFERENCES citus_local_table_4(l1);

-- check stats creation
CREATE TABLE citus_local_table_stats2(a int, b int);
CREATE STATISTICS stx8 ON a, b FROM citus_local_table_stats2;
SELECT citus_add_local_table_to_metadata('citus_local_table_stats2');
CREATE STATISTICS stx9 ON a, b FROM citus_local_table_stats2;
DROP STATISTICS stx8;
DROP STATISTICS stx4;

SELECT stxname FROM pg_statistic_ext ORDER BY stxname;

\c - - - :master_port
SET search_path TO citus_local_tables_mx;

-- undistribute old tables to prevent unnecessary undistribute logs later
SELECT undistribute_table('citus_local_table', cascade_via_foreign_keys=>true);
SELECT undistribute_table('citus_local_table_3', cascade_via_foreign_keys=>true);
SELECT undistribute_table('citus_local_table_stats', cascade_via_foreign_keys=>true);

-- verify that mx nodes have the shell table for partitioned citus local tables
CREATE TABLE local_table_fkey(a INT UNIQUE);
CREATE TABLE local_table_fkey2(a INT UNIQUE);
CREATE TABLE partitioned_mx (a INT UNIQUE) PARTITION BY RANGE(a);
ALTER TABLE partitioned_mx ADD CONSTRAINT fkey_to_local FOREIGN KEY (a) REFERENCES local_table_fkey(a);

CREATE TABLE IF NOT EXISTS partitioned_mx_1 PARTITION OF partitioned_mx FOR VALUES FROM (1) TO (4);
CREATE TABLE partitioned_mx_2 (a INT UNIQUE);
ALTER TABLE partitioned_mx ATTACH PARTITION partitioned_mx_2 FOR VALUES FROM (5) TO (8);
SELECT create_reference_table('local_table_fkey');
CREATE TABLE partitioned_mx_3 (a INT UNIQUE);
ALTER TABLE partitioned_mx ATTACH PARTITION partitioned_mx_3 FOR VALUES FROM (9) TO (12);
-- these should error out since multi-level partitioned citus local tables are not supported
CREATE TABLE IF NOT EXISTS partitioned_mx_4 PARTITION OF partitioned_mx FOR VALUES FROM (13) TO (16) PARTITION BY RANGE (a);
BEGIN;
    CREATE TABLE partitioned_mx_4(a INT UNIQUE) PARTITION BY RANGE (a);
    alter table partitioned_mx attach partition partitioned_mx_4 FOR VALUES FROM (13) TO (16);
END;
CREATE TABLE multi_level_p (a INT UNIQUE) PARTITION BY RANGE (a);
CREATE TABLE IF NOT EXISTS multi_level_c PARTITION OF multi_level_p FOR VALUES FROM (13) TO (16) PARTITION BY RANGE (a);
select citus_add_local_table_to_metadata('multi_level_p'); --errors
select citus_add_local_table_to_metadata('multi_level_p', true); --errors
-- try attaching a partition with an external foreign key
CREATE TABLE partitioned_mx_4 (a INT UNIQUE);
ALTER TABLE partitioned_mx_4 ADD CONSTRAINT fkey_not_inherited FOREIGN KEY (a) REFERENCES local_table_fkey2(a);
-- these two should error out
ALTER TABLE partitioned_mx ATTACH PARTITION partitioned_mx_4 FOR VALUES FROM (13) TO (16);
ALTER TABLE partitioned_mx ATTACH PARTITION partitioned_mx_4 default;

SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname      AS parent,
    nmsp_child.nspname  AS child_schema,
    child.relname       AS child
FROM pg_inherits
    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE parent.relname='partitioned_mx'
ORDER BY child;

\c - - - :worker_1_port
SELECT relname FROM pg_class WHERE relname LIKE 'partitioned_mx%' ORDER BY relname;
SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname      AS parent,
    nmsp_child.nspname  AS child_schema,
    child.relname       AS child
FROM pg_inherits
    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE parent.relname='partitioned_mx'
ORDER BY child;

\c - - - :master_port
SET search_path TO citus_local_tables_mx;
SET client_min_messages TO ERROR;
DROP TABLE partitioned_mx;
RESET client_min_messages;
-- test cascading via foreign keys
CREATE TABLE cas_1 (a INT UNIQUE);
CREATE TABLE cas_par (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE cas_par_1 PARTITION OF cas_par FOR VALUES FROM (1) TO (4);
CREATE TABLE cas_par_2 PARTITION OF cas_par FOR VALUES FROM (5) TO (8);
ALTER TABLE cas_par_1 ADD CONSTRAINT fkey_cas_test_1 FOREIGN KEY (a) REFERENCES cas_1(a);
CREATE TABLE cas_par2 (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE cas_par2_1 PARTITION OF cas_par2 FOR VALUES FROM (1) TO (4);
CREATE TABLE cas_par2_2 PARTITION OF cas_par2 FOR VALUES FROM (5) TO (8);
ALTER TABLE cas_par2_1 ADD CONSTRAINT fkey_cas_test_2 FOREIGN KEY (a) REFERENCES cas_1(a);
CREATE TABLE cas_par3 (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE cas_par3_1 PARTITION OF cas_par3 FOR VALUES FROM (1) TO (4);
CREATE TABLE cas_par3_2 PARTITION OF cas_par3 FOR VALUES FROM (5) TO (8);
-- these two should error out as we should call the conversion from the parent
SELECT citus_add_local_table_to_metadata('cas_par2_2');
SELECT citus_add_local_table_to_metadata('cas_par2_2', cascade_via_foreign_keys=>true);
-- these two should error out as the foreign keys are not inherited from the parent
SELECT citus_add_local_table_to_metadata('cas_par2');
SELECT citus_add_local_table_to_metadata('cas_par2', cascade_via_foreign_keys=>true);
-- drop the foreign keys and establish them again using the parent table
ALTER TABLE cas_par_1 DROP CONSTRAINT fkey_cas_test_1;
ALTER TABLE cas_par2_1 DROP CONSTRAINT fkey_cas_test_2;
ALTER TABLE cas_par ADD CONSTRAINT fkey_cas_test_1 FOREIGN KEY (a) REFERENCES cas_1(a);
ALTER TABLE cas_par2 ADD CONSTRAINT fkey_cas_test_2 FOREIGN KEY (a) REFERENCES cas_1(a);
ALTER TABLE cas_par3 ADD CONSTRAINT fkey_cas_test_3 FOREIGN KEY (a) REFERENCES cas_par(a);
-- this should error out as cascade_via_foreign_keys is not set to true
SELECT citus_add_local_table_to_metadata('cas_par2');
-- this should work
SELECT citus_add_local_table_to_metadata('cas_par2', cascade_via_foreign_keys=>true);
-- verify the partitioning hierarchy is preserved
select inhrelid::regclass from pg_inherits where inhparent='cas_par'::regclass order by 1;
-- verify the fkeys + fkeys with shard ids are created
select conname from pg_constraint where conname like 'fkey_cas_test%' order by conname;
-- when all partitions are converted, there should be 40 tables and indexes
-- the individual names are not much relevant, so we only print the count
SELECT count(*) FROM pg_class WHERE relname LIKE 'cas\_%' AND relnamespace IN
    (SELECT oid FROM pg_namespace WHERE nspname = 'citus_local_tables_mx');
-- verify on the mx worker
\c - - - :worker_1_port
-- on worker, there should be 20, since the shards are created only on the coordinator
SELECT count(*) FROM pg_class WHERE relname LIKE 'cas\_%' AND relnamespace IN
    (SELECT oid FROM pg_namespace WHERE nspname = 'citus_local_tables_mx');
-- verify that the shell foreign keys are created on the worker as well
select conname from pg_constraint where conname like 'fkey_cas_test%' order by conname;
\c - - - :master_port
SET search_path TO citus_local_tables_mx;
-- undistribute table
-- this one should error out since we don't set the cascade option as true
SELECT undistribute_table('cas_par2');
-- this one should work
SET client_min_messages TO WARNING;
SELECT undistribute_table('cas_par2', cascade_via_foreign_keys=>true);
-- verify the partitioning hierarchy is preserved
select inhrelid::regclass from pg_inherits where inhparent='cas_par'::regclass order by 1;
-- verify that the foreign keys with shard ids are gone, due to undistribution
select conname from pg_constraint where conname like 'fkey_cas_test%' order by conname;
-- add a non-inherited fkey and verify it fails when trying to convert
ALTER TABLE cas_par2_1 ADD CONSTRAINT fkey_cas_test_3 FOREIGN KEY (a) REFERENCES cas_1(a);
SELECT citus_add_local_table_to_metadata('cas_par2', cascade_via_foreign_keys=>true);
-- verify undistribute_table works proper for the mx worker
\c - - - :worker_1_port
SELECT relname FROM pg_class WHERE relname LIKE 'cas\_%' ORDER BY relname;
\c - - - :master_port
SET search_path TO citus_local_tables_mx;

CREATE TABLE date_partitioned_citus_local_table_seq( measureid bigserial, eventdate date, measure_data jsonb, PRIMARY KEY (measureid, eventdate)) PARTITION BY RANGE(eventdate);
SELECT create_time_partitions('date_partitioned_citus_local_table_seq', INTERVAL '1 month', '2022-01-01', '2021-01-01');
SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table_seq');
DROP TABLE date_partitioned_citus_local_table_seq;
-- test sequences
CREATE TABLE par_citus_local_seq(measureid bigserial, val int) PARTITION BY RANGE(val);
CREATE TABLE par_citus_local_seq_1 PARTITION OF par_citus_local_seq FOR VALUES FROM (1) TO (4);
SELECT citus_add_local_table_to_metadata('par_citus_local_seq');
INSERT INTO par_citus_local_seq (val) VALUES (1) RETURNING *;
INSERT INTO par_citus_local_seq (val) VALUES (2) RETURNING *;
\c - - - :worker_1_port
-- insert on the worker
INSERT INTO citus_local_tables_mx.par_citus_local_seq (val) VALUES (1) RETURNING *;
\c - - - :master_port
SET search_path TO citus_local_tables_mx;
INSERT INTO par_citus_local_seq (val) VALUES (2) RETURNING *;
SELECT undistribute_table('par_citus_local_seq');
INSERT INTO par_citus_local_seq (val) VALUES (3) RETURNING *;
SELECT measureid FROM par_citus_local_seq ORDER BY measureid;

-- test adding invalid foreign key to partition table
CREATE TABLE citus_local_parent_1 (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE citus_local_parent_2 (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE citus_local_plain (a INT UNIQUE);
CREATE TABLE ref (a INT UNIQUE);
SELECT create_reference_table('ref');
alter table citus_local_parent_1 add foreign key (a) references citus_local_parent_2(a);
alter table citus_local_plain add foreign key (a) references citus_local_parent_2(a);
CREATE TABLE citus_local_parent_1_child_1 PARTITION OF citus_local_parent_1 FOR VALUES FROM (3) TO (5);
CREATE TABLE citus_local_parent_1_child_2 PARTITION OF citus_local_parent_1 FOR VALUES FROM (30) TO (50);
CREATE TABLE citus_local_parent_2_child_1 PARTITION OF citus_local_parent_2 FOR VALUES FROM (40) TO (60);
-- this one should error out, since we cannot convert it to citus local table,
-- as citus local table partitions cannot have non-inherited foreign keys
alter table citus_local_parent_1_child_1 add foreign key(a) references ref(a);
-- this should work
alter table citus_local_parent_1 add constraint fkey_to_drop_test foreign key(a) references ref(a);
-- this should undistribute the table, and the entries should be gone from pg_dist_partition
select logicalrelid from pg_dist_partition where logicalrelid::text like 'citus_local_parent%' order by logicalrelid;
set client_min_messages to error;
alter table citus_local_parent_1 drop constraint fkey_to_drop_test;
select logicalrelid from pg_dist_partition where logicalrelid::text like 'citus_local_parent%';

-- verify attaching partition with a foreign key errors out
CREATE TABLE citus_local_parent (b TEXT, a INT UNIQUE REFERENCES ref(a), d INT) PARTITION BY RANGE(a);
CREATE TABLE citus_local_with_fkey (d INT);
ALTER TABLE citus_local_with_fkey ADD CONSTRAINT cl_to_cl FOREIGN KEY(d) REFERENCES citus_local_parent(a);
-- error out
ALTER TABLE citus_local_parent ATTACH PARTITION citus_local_with_fkey DEFAULT;
DROP TABLE citus_local_parent CASCADE;

-- test attaching citus local table to distributed table
-- citus local table should be distributed
CREATE TABLE dist_table_parent (a INT UNIQUE) PARTITION BY RANGE(a);
SELECT create_distributed_table('dist_table_parent','a');
CREATE TABLE citus_local_child (a int unique);
select citus_add_local_table_to_metadata('citus_local_child', false);
alter table dist_table_parent attach partition  citus_local_child default ;
select logicalrelid, partmethod from pg_dist_partition where logicalrelid::text in ('dist_table_parent', 'citus_local_child');

-- test attaching distributed table to citus local table
CREATE TABLE dist_table_child (a INT UNIQUE);
SELECT create_distributed_table('dist_table_child','a');
CREATE TABLE citus_local_parent (a INT UNIQUE) PARTITION BY RANGE(a);
select citus_add_local_table_to_metadata('citus_local_parent', false);
-- this should error out
alter table citus_local_parent attach partition dist_table_child default ;

-- error out attaching
CREATE TABLE pg_local (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE citus_local_attach_to_pglocal (a INT UNIQUE) PARTITION BY RANGE(a);
select citus_add_local_table_to_metadata('citus_local_attach_to_pglocal', false);
alter table citus_local_attach_to_pglocal attach partition pg_local default ;

SELECT master_remove_distributed_table_metadata_from_workers('citus_local_table_4'::regclass::oid, 'citus_local_tables_mx', 'citus_local_table_4');

-- both workers should print 0 as master_remove_distributed_table_metadata_from_workers
-- drops the table as well
SELECT run_command_on_workers(
$$
SELECT count(*) FROM pg_catalog.pg_tables WHERE tablename='citus_local_table_4'
$$);

-- verify that partitioned citus local tables with dropped columns can be distributed. issue: #5577
CREATE TABLE parent_dropped_col(a int, eventtime date) PARTITION BY RANGE ( eventtime);
SELECT citus_add_local_table_to_metadata('parent_dropped_col');
ALTER TABLE parent_dropped_col DROP column a;
CREATE TABLE parent_dropped_col_1 PARTITION OF parent_dropped_col for VALUES FROM ('2000-01-01') TO ('2001-01-01');
SELECT create_distributed_table('parent_dropped_col', 'eventtime');
-- another example to test
CREATE TABLE parent_dropped_col_2(
  col_to_drop_0 text,
  col_to_drop_1 text,
  col_to_drop_2 date,
  col_to_drop_3 inet,
  col_to_drop_4 date,
  measureid integer,
  eventdatetime date,
  measure_data jsonb,
  PRIMARY KEY (measureid, eventdatetime, measure_data))
  PARTITION BY RANGE(eventdatetime);

select citus_add_local_table_to_metadata('parent_dropped_col_2');
ALTER TABLE parent_dropped_col_2 DROP COLUMN col_to_drop_1;
CREATE TABLE parent_dropped_col_2_2000 PARTITION OF parent_dropped_col_2 FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');

SELECT create_distributed_table('parent_dropped_col_2', 'measureid');

-- verify that the partitioned tables are distributed with the correct distribution column
SELECT logicalrelid, partmethod, partkey FROM pg_dist_partition
    WHERE logicalrelid IN ('parent_dropped_col'::regclass, 'parent_dropped_col_2'::regclass)
        ORDER BY logicalrelid;

-- some tests for view propagation on citus local tables
CREATE TABLE view_tbl_1 (a int);
CREATE TABLE view_tbl_2 (a int);
CREATE SCHEMA viewsc;
-- create dependent views, in a different schema
-- the first one depends on a citus metadata table
CREATE VIEW viewsc.prop_view AS SELECT COUNT (*) FROM view_tbl_1 JOIN pg_dist_node ON view_tbl_1.a=pg_dist_node.nodeid;
CREATE VIEW viewsc.prop_view2 AS SELECT COUNT (*) FROM view_tbl_1;
SELECT citus_add_local_table_to_metadata('view_tbl_1');
-- verify the shard view is dropped, and created&propagated the correct view
SELECT viewname, definition FROM pg_views WHERE viewname LIKE 'prop_view%' ORDER BY viewname;

SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'prop_view%';$$);
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid IN('viewsc.prop_view'::regclass::oid, 'viewsc.prop_view2'::regclass::oid) ORDER BY 1;
-- drop views
DROP VIEW viewsc.prop_view;
DROP VIEW viewsc.prop_view2;
-- verify dropped on workers
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'prop_view%';$$);

-- create a view that depends on a pg_ table
CREATE VIEW viewsc.prop_view3 AS SELECT COUNT (*) FROM view_tbl_1 JOIN pg_namespace ON view_tbl_1.a=pg_namespace.nspowner;
-- create a view that depends on two different tables, one of them is local for now
CREATE VIEW viewsc.prop_view4 AS SELECT COUNT (*) FROM view_tbl_1 JOIN view_tbl_2 ON view_tbl_1.a=view_tbl_2.a;
-- distribute the first table
SELECT create_distributed_table('view_tbl_1','a');
-- verify the last view is not distributed
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'prop_view%';$$);
-- add the other table to metadata, so the local view gets distributed
SELECT citus_add_local_table_to_metadata('view_tbl_2');
-- verify both views are distributed
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_views WHERE viewname LIKE 'prop_view%';$$);
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid IN('viewsc.prop_view3'::regclass::oid, 'viewsc.prop_view4'::regclass::oid) ORDER BY 1;

-- test with fkey cascading
create table ref_tb(a int primary key);
SELECT create_reference_table('ref_tb');

CREATE TABLE loc_tb (a int );

CREATE VIEW v100 AS SELECT * FROM loc_tb;
CREATE VIEW v101 AS SELECT * FROM loc_tb JOIN ref_tb USING (a);
CREATE VIEW v102 AS SELECT * FROM v101;

-- a regular matview that depends on local table
CREATE MATERIALIZED VIEW matview_101 AS SELECT * from loc_tb;
-- a matview and a view that depend on the local table + each other
CREATE VIEW v103 AS SELECT * from loc_tb;
CREATE MATERIALIZED VIEW matview_102 AS SELECT * from loc_tb JOIN v103 USING (a);
CREATE OR REPLACE VIEW v103 AS SELECT * from loc_tb JOIN matview_102 USING (a);

-- fails to add local table to metadata, because of the circular dependency
ALTER TABLE loc_tb ADD CONSTRAINT fkey FOREIGN KEY (a) references ref_tb(a);
-- drop the view&matview with circular dependency
DROP VIEW v103 CASCADE;

-- now it should successfully add to metadata and create the views on workers
ALTER TABLE loc_tb ADD CONSTRAINT fkey FOREIGN KEY (a) references ref_tb(a);
-- verify the views are created on workers
select run_command_on_workers($$SELECT count(*)=0 from citus_local_tables_mx.v100$$);
select run_command_on_workers($$SELECT count(*)=0 from citus_local_tables_mx.v101$$);
select run_command_on_workers($$SELECT count(*)=0 from citus_local_tables_mx.v102$$);

CREATE TABLE loc_tb_2 (a int);
CREATE VIEW v104 AS SELECT * from loc_tb_2 table_name_for_view;

SET client_min_messages TO DEBUG1;
-- verify the CREATE command for the view is generated correctly
ALTER TABLE loc_tb_2 ADD CONSTRAINT fkey_2 FOREIGN KEY (a) references ref_tb(a);
SET client_min_messages TO WARNING;

-- works fine
select run_command_on_workers($$SELECT count(*) from citus_local_tables_mx.v100, citus_local_tables_mx.v101, citus_local_tables_mx.v102$$);

-- auto undistribute
ALTER TABLE loc_tb DROP CONSTRAINT fkey;
-- fails because fkey is dropped and table is converted to local table
select run_command_on_workers($$SELECT count(*) from citus_local_tables_mx.v100$$);
select run_command_on_workers($$SELECT count(*) from citus_local_tables_mx.v101$$);
select run_command_on_workers($$SELECT count(*) from citus_local_tables_mx.v102$$);

INSERT INTO loc_tb VALUES (1), (2);
-- test a matview with columnar
CREATE MATERIALIZED VIEW matview_columnar USING COLUMNAR AS SELECT * FROM loc_tb WITH DATA;

-- cant recreate matviews, because the size limit is set to zero, by the GUC
SET citus.max_matview_size_to_auto_recreate TO 0;
SELECT citus_add_local_table_to_metadata('loc_tb', true);
-- remove the limit
SET citus.max_matview_size_to_auto_recreate TO -1;
SELECT citus_add_local_table_to_metadata('loc_tb', true);

-- test REFRESH MAT VIEW
SELECT * FROM matview_101 ORDER BY a;
REFRESH MATERIALIZED VIEW matview_101;
SELECT * FROM matview_101 ORDER BY a;

-- verify columnar matview works on a table added to metadata
SELECT * FROM matview_columnar;
REFRESH MATERIALIZED VIEW matview_columnar;
SELECT * FROM matview_columnar ORDER BY a;

-- test with partitioned tables
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE parent_1 (a INT UNIQUE) PARTITION BY RANGE(a);
SET citus.use_citus_managed_tables TO OFF;

CREATE MATERIALIZED VIEW part_matview1 as SELECT count(*) FROM parent_1 JOIN parent_1 p2 ON (true);
CREATE MATERIALIZED VIEW part_matview2 as SELECT count(*) FROM parent_1 JOIN part_matview1 on (true);

SELECT count(*) FROM citus_local_tables_mx.part_matview1 JOIN citus_local_tables_mx.part_matview2 ON (true);

CREATE TABLE parent_1_child_1 (a int);
CREATE TABLE parent_1_child_2 (a int);

-- create matviews on partition tables
CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM parent_1_child_1;
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM parent_1_child_2;
CREATE MATERIALIZED VIEW mv3 AS SELECT parent_1_child_2.* FROM parent_1_child_2 JOIN parent_1_child_1 USING(a);
CREATE MATERIALIZED VIEW mv4 AS SELECT * FROM mv3;

alter table parent_1 attach partition parent_1_child_1 FOR VALUES FROM (0) TO (10) ;

-- all matviews work
SELECT count(*) FROM citus_local_tables_mx.mv1;
SELECT count(*) FROM citus_local_tables_mx.mv2;
SELECT count(*) FROM citus_local_tables_mx.mv3;
SELECT count(*) FROM citus_local_tables_mx.mv4;

-- recreate matviews and verify they still work
alter table parent_1 attach partition parent_1_child_2 FOR VALUES FROM (10) TO (20);

SELECT count(*) FROM citus_local_tables_mx.mv1;
SELECT count(*) FROM citus_local_tables_mx.mv2;
SELECT count(*) FROM citus_local_tables_mx.mv3;
SELECT count(*) FROM citus_local_tables_mx.mv4;

-- verify matviews work after undistributing
SELECT undistribute_table('parent_1');
SELECT count(*) FROM citus_local_tables_mx.mv1;
SELECT count(*) FROM citus_local_tables_mx.mv2;
SELECT count(*) FROM citus_local_tables_mx.mv3;
SELECT count(*) FROM citus_local_tables_mx.mv4;

-- test circular dependency detection among views
create table root_tbl (a int);
create materialized view chain_v1 as select * from root_tbl;
create view chain_v2 as select * from chain_v1;
create materialized view chain_v3 as select * from chain_v2;
create or replace view chain_v2 as select * from chain_v1 join chain_v3 using (a);
-- catch circular dependency and error out
select citus_add_local_table_to_metadata('root_tbl');
-- same for create_distributed_table
select create_distributed_table('root_tbl','a');
-- fix the circular dependency and add to metadata
create or replace view chain_v2 as select * from chain_v1;
select citus_add_local_table_to_metadata('root_tbl');
-- todo: add more matview tests once 5968 and 6028 are fixed

-- cleanup at exit
set client_min_messages to error;
DROP SCHEMA citus_local_tables_mx CASCADE;
