-- regression tests regarding foreign key
-- drops cascading into undistributing Citus
-- local tables to Postgres local tables
CREATE SCHEMA drop_fkey_cascade;
SET search_path TO drop_fkey_cascade;
SET client_min_messages TO WARNING;
SET citus.next_shard_id TO 1810000;
SET citus.next_placement_id TO 3070000;

SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

-- show that DROP CONSTRAINT cascades to undistributing citus_local_table
CREATE TABLE citus_local_table(l1 int);
CREATE TABLE reference_table(r1 int primary key);
SELECT create_reference_table('reference_table');
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE CASCADE;

SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

CREATE OR REPLACE FUNCTION drop_constraint_cascade_via_perform_deletion(IN table_name regclass, IN constraint_name text)
RETURNS VOID
LANGUAGE C STRICT
AS 'citus', $$drop_constraint_cascade_via_perform_deletion$$;

BEGIN;
  SELECT drop_constraint_cascade_via_perform_deletion('citus_local_table', 'fkey_local_to_ref');
  -- we dropped constraint without going through utility hook,
  -- so we should still see citus_local_table
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
ROLLBACK;

ALTER TABLE citus_local_table DROP CONSTRAINT fkey_local_to_ref;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

DROP TABLE citus_local_table, reference_table;

-- show that DROP COLUMN cascades to undistributing citus_local_table
CREATE TABLE reference_table(r1 int primary key, r2 int);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

ALTER TABLE reference_table DROP COLUMN r1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
DROP TABLE citus_local_table, reference_table;

-- show that DROP COLUMN that cascades into drop foreign key undistributes local table
CREATE TABLE reference_table(r1 int primary key, r2 int);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
ALTER TABLE citus_local_table DROP COLUMN l1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;


DROP TABLE citus_local_table, reference_table;

-- show that PRIMARY KEY that cascades into drop foreign key undistributes local table
CREATE TABLE reference_table(r1 int primary key, r2 int);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
ALTER TABLE reference_table DROP CONSTRAINT reference_table_pkey CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

-- show that DROP UNIQUE INDEX that cascades into drop foreign key undistributes local table
DROP TABLE citus_local_table, reference_table;

CREATE TABLE reference_table(r1 int, r2 int);
SELECT create_reference_table('reference_table');
CREATE UNIQUE INDEX ref_unique ON reference_table(r1);

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
DROP INDEX ref_unique CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

-- show that UNIQUE CONSTRAINT that cascades into drop foreign key undistributes local table
DROP TABLE citus_local_table, reference_table;

CREATE TABLE reference_table(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
ALTER TABLE reference_table DROP CONSTRAINT reference_table_r1_key  CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;

-- show that DROP TABLE that cascades into drop foreign key undistributes local table
DROP TABLE citus_local_table, reference_table;

CREATE TABLE reference_table(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table');
CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
DROP TABLE reference_table CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass) ORDER BY logicalrelid;


-- show that UNIQUE CONSTRAINT that cascades into drop foreign key undistributes local table
DROP TABLE citus_local_table;

CREATE TABLE reference_table(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table(l1 int REFERENCES reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;
ALTER TABLE reference_table DROP CONSTRAINT reference_table_r1_key  CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table'::regclass) ORDER BY logicalrelid;


-- show that DROP SCHEMA that cascades into drop foreign key undistributes local table
DROP TABLE citus_local_table, reference_table;

CREATE SCHEMA ref_table_drop_schema;
CREATE TABLE ref_table_drop_schema.reference_table(r1 int UNIQUE, r2 int);
SELECT create_reference_table('ref_table_drop_schema.reference_table');
CREATE TABLE citus_local_table(l1 int REFERENCES ref_table_drop_schema.reference_table(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'ref_table_drop_schema.reference_table'::regclass) ORDER BY logicalrelid;
DROP SCHEMA ref_table_drop_schema CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass) ORDER BY logicalrelid;


-- drop column cascade that doesn't cascade into citus local table
DROP TABLE IF EXISTS citus_local_table, reference_table_1, reference_table_2;

CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE reference_table_2(r1 int UNIQUE REFERENCES reference_table_1(r1), r2 int);
SELECT create_reference_table('reference_table_2');
CREATE TABLE citus_local_table(l1 int REFERENCES reference_table_2(r1), l2 int);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;
ALTER TABLE reference_table_1 DROP COLUMN r1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;


-- local table has multiple foreign keys to two tables
-- drop one at a time
DROP TABLE IF EXISTS citus_local_table, reference_table_1, reference_table_2;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE reference_table_2(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_2');
CREATE TABLE citus_local_table(l1 int REFERENCES reference_table_1(r1), l2 int REFERENCES reference_table_2(r1));
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;

DROP TABLE reference_table_1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;

CREATE TABLE distributed_table (d1 int);
SELECT create_distributed_table('distributed_table', 'd1');

-- drop an unrelated distributed table too
DROP TABLE reference_table_2, distributed_table CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass) ORDER BY logicalrelid;

-- local table has multiple foreign keys to two tables
-- drop both at the same time
DROP TABLE IF EXISTS citus_local_table, reference_table_1, reference_table_2;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE reference_table_2(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_2');
CREATE TABLE citus_local_table(l1 int REFERENCES reference_table_1(r1), l2 int REFERENCES reference_table_2(r1));
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;
DROP TABLE reference_table_1, reference_table_2 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass) ORDER BY logicalrelid;

-- local table has multiple foreign keys to two tables
-- drop one at a time
DROP TABLE IF EXISTS citus_local_table, reference_table_1, reference_table_2;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE reference_table_2(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_2');
CREATE TABLE citus_local_table(l1 int REFERENCES reference_table_1(r1), l2 int REFERENCES reference_table_2(r1));
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;

BEGIN;
  ALTER TABLE citus_local_table DROP CONSTRAINT citus_local_table_l1_fkey;
  SAVEPOINT sp1;

  -- this should undistribute citus_local_table
  ALTER TABLE citus_local_table DROP CONSTRAINT citus_local_table_l2_fkey;
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;
  ROLLBACK TO SAVEPOINT sp1;

  -- rollback'ed second drop constraint, so we should still see citus_local_table
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;

  -- this should undistribute citus_local_table again
  ALTER TABLE citus_local_table DROP CONSTRAINT citus_local_table_l2_fkey;
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('citus_local_table'::regclass, 'reference_table_1'::regclass, 'reference_table_2'::regclass) ORDER BY logicalrelid;
COMMIT;

-- a single drop column cascades into multiple undistributes
DROP TABLE IF EXISTS citus_local_table_1, citus_local_table_2, reference_table_1;

CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');

CREATE TABLE citus_local_table_1(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE);
CREATE TABLE citus_local_table_2(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE REFERENCES citus_local_table_1(l2));
CREATE TABLE citus_local_table_3(l1 int REFERENCES reference_table_1(r1), l2 int REFERENCES citus_local_table_2(l2));
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass, 'citus_local_table_2'::regclass, 'citus_local_table_3'::regclass) ORDER BY logicalrelid;
ALTER TABLE reference_table_1 DROP COLUMN r1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass, 'citus_local_table_2'::regclass, 'citus_local_table_3'::regclass) ORDER BY logicalrelid;

-- verify that citus local tables converted by the user will not be auto-undistributed
DROP TABLE IF EXISTS citus_local_table_1, citus_local_table_2, citus_local_table_3;

-- this GUC will add the next three tables to metadata automatically
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE citus_local_table_1(a INT UNIQUE);
CREATE TABLE citus_local_table_2(a INT UNIQUE);
CREATE TABLE citus_local_table_3(a INT UNIQUE);
RESET citus.use_citus_managed_tables;
ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_cas_test FOREIGN KEY (a) REFERENCES citus_local_table_2 (a);
ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_cas_test_2 FOREIGN KEY (a) REFERENCES citus_local_table_2 (a);
ALTER TABLE citus_local_table_3 DROP CONSTRAINT fkey_cas_test_2;
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('citus_local_table_1'::regclass,
                         'citus_local_table_2'::regclass,
                         'citus_local_table_3'::regclass)
  ORDER BY logicalrelid;
ALTER TABLE citus_local_table_1 DROP CONSTRAINT fkey_cas_test;
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('citus_local_table_1'::regclass,
                         'citus_local_table_2'::regclass,
                         'citus_local_table_3'::regclass)
  ORDER BY logicalrelid;

-- verify that tables that are connected to reference tables are marked as autoConverted = true
CREATE TABLE ref_test(a int UNIQUE);
SELECT create_reference_table('ref_test');
CREATE TABLE auto_local_table_1(a int UNIQUE);
CREATE TABLE auto_local_table_2(a int UNIQUE REFERENCES auto_local_table_1(a));
ALTER TABLE auto_local_table_1 ADD CONSTRAINT fkey_to_ref_tbl FOREIGN KEY (a) REFERENCES ref_test(a);

SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('auto_local_table_1'::regclass,
                         'auto_local_table_2'::regclass)
  ORDER BY logicalrelid;

-- verify that we can mark both of them with autoConverted = false, by converting one of them manually
SELECT citus_add_local_table_to_metadata('auto_local_table_1');
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('auto_local_table_1'::regclass,
                         'auto_local_table_2'::regclass)
  ORDER BY logicalrelid;

-- test with partitioned tables
CREATE TABLE partitioned_table_1 (a int unique) partition by range(a);
CREATE TABLE partitioned_table_1_1 partition of partitioned_table_1 FOR VALUES FROM (0) TO (10);
CREATE TABLE partitioned_table_2 (a int unique) partition by range(a);
CREATE TABLE partitioned_table_2_1 partition of partitioned_table_2 FOR VALUES FROM (0) TO (10);
CREATE TABLE ref_fkey_to_partitioned (a int unique references partitioned_table_1(a), b int unique references partitioned_table_2(a));
SELECT create_reference_table('ref_fkey_to_partitioned');

-- verify that partitioned tables and partitions are converted automatically
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('partitioned_table_1'::regclass,
                         'partitioned_table_1_1'::regclass,
                         'partitioned_table_2'::regclass,
                         'partitioned_table_2_1'::regclass)
  ORDER BY logicalrelid;

BEGIN;
  SELECT citus_add_local_table_to_metadata('partitioned_table_2');

  -- verify that they are now marked as auto-converted = false
  SELECT logicalrelid, autoconverted FROM pg_dist_partition
    WHERE logicalrelid IN ('partitioned_table_1'::regclass,
                          'partitioned_table_1_1'::regclass,
                          'partitioned_table_2'::regclass,
                          'partitioned_table_2_1'::regclass)
    ORDER BY logicalrelid;
ROLLBACK;

-- now they should be undistributed
DROP TABLE ref_fkey_to_partitioned;
-- verify that they are undistributed
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('partitioned_table_1'::regclass,
                        'partitioned_table_1_1'::regclass,
                        'partitioned_table_2'::regclass,
                        'partitioned_table_2_1'::regclass)
  ORDER BY logicalrelid;

-- verify creating fkeys update auto-converted to false
CREATE TABLE table_ref(a int unique);
CREATE TABLE table_auto_conv(a int unique references table_ref(a)) partition by range(a);
CREATE TABLE table_auto_conv_child partition of table_auto_conv FOR VALUES FROM (1) TO (4);
CREATE TABLE table_auto_conv_2(a int unique references table_auto_conv(a));
CREATE TABLE table_not_auto_conv(a int unique);
select create_reference_table('table_ref');

-- table_not_auto_conv should not be here, as it's not converted yet
-- other tables should be marked as auto-converted
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('table_auto_conv'::regclass,
                        'table_auto_conv_child'::regclass,
                        'table_auto_conv_2'::regclass,
                        'table_not_auto_conv'::regclass)
  ORDER BY logicalrelid;

select citus_add_local_table_to_metadata('table_not_auto_conv');
alter table table_not_auto_conv add constraint fkey_to_mark_not_autoconverted foreign key (a) references table_auto_conv_2(a);

-- all of them should be marked as auto-converted = false
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('table_auto_conv'::regclass,
                        'table_auto_conv_child'::regclass,
                        'table_auto_conv_2'::regclass,
                        'table_not_auto_conv'::regclass)
  ORDER BY logicalrelid;

-- create&attach new partition, it should be marked as auto-converted = false, too
CREATE TABLE table_auto_conv_child_2 partition of table_auto_conv FOR VALUES FROM (5) TO (8);
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('table_auto_conv'::regclass,
                        'table_auto_conv_child'::regclass,
                        'table_auto_conv_2'::regclass,
                        'table_not_auto_conv'::regclass,
                        'table_auto_conv_child_2'::regclass)
  ORDER BY logicalrelid;

-- get the autoconverted field from the parent in case of
-- CREATE TABLE .. PARTITION OF ..
create table citus_local_parent_t(a int, b int REFERENCES table_ref(a)) PARTITION BY RANGE (b);
create table citus_child_t  PARTITION OF citus_local_parent_t FOR VALUES FROM (1) TO (10);
-- should be set to true
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('citus_local_parent_t'::regclass,
                        'citus_child_t'::regclass)
  ORDER BY logicalrelid;

-- test CREATE TABLE REFERENCES
CREATE TABLE citus_local_user_created(a int unique);
SELECT citus_add_local_table_to_metadata('citus_local_user_created');
CREATE TABLE citus_local_references(a int unique references citus_local_user_created(a));
SELECT logicalrelid, autoconverted FROM pg_dist_partition
  WHERE logicalrelid IN ('citus_local_user_created'::regclass,
                        'citus_local_references'::regclass)
  ORDER BY logicalrelid;

SET citus.shard_replication_factor to 1;
-- test with a graph that includes distributed table
CREATE TABLE distr_table (a INT UNIQUE);
SELECT create_distributed_table('distr_table','a');

-- test converting in create_reference_table time
CREATE TABLE refr_table (a INT UNIQUE, b INT UNIQUE);
CREATE TABLE citus_loc_1 (a INT UNIQUE REFERENCES refr_table(a), b INT UNIQUE);
CREATE TABLE citus_loc_2 (a INT UNIQUE REFERENCES citus_loc_1(a), b INT UNIQUE REFERENCES citus_loc_1(b), c INT UNIQUE REFERENCES refr_table(a));
CREATE TABLE citus_loc_3 (a INT UNIQUE REFERENCES citus_loc_3(a));
CREATE TABLE citus_loc_4 (a INT UNIQUE REFERENCES citus_loc_2(b), b INT UNIQUE REFERENCES citus_loc_2(a), c INT UNIQUE REFERENCES citus_loc_3(a));
ALTER TABLE refr_table ADD CONSTRAINT fkey_ref_to_loc FOREIGN KEY (b) REFERENCES citus_loc_2(a);
SELECT create_reference_table('refr_table');
ALTER TABLE distr_table ADD CONSTRAINT fkey_dist_to_ref FOREIGN KEY (a) REFERENCES refr_table(a);

SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
  WHERE logicalrelid IN ('distr_table'::regclass,
                         'citus_loc_1'::regclass,
                         'citus_loc_2'::regclass,
                         'citus_loc_3'::regclass,
                         'citus_loc_4'::regclass,
                         'refr_table'::regclass)
  ORDER BY logicalrelid;

BEGIN;
  SELECT citus_add_local_table_to_metadata('citus_loc_3');
  SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
    WHERE logicalrelid IN ('distr_table'::regclass,
                          'citus_loc_1'::regclass,
                          'citus_loc_2'::regclass,
                          'citus_loc_3'::regclass,
                          'citus_loc_4'::regclass,
                          'refr_table'::regclass)
    ORDER BY logicalrelid;
ROLLBACK;

SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
  WHERE logicalrelid IN ('distr_table'::regclass,
                         'citus_loc_1'::regclass,
                         'citus_loc_2'::regclass,
                         'citus_loc_3'::regclass,
                         'citus_loc_4'::regclass,
                         'refr_table'::regclass)
  ORDER BY logicalrelid;

BEGIN;
  SELECT citus_add_local_table_to_metadata('citus_loc_2');
  SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
    WHERE logicalrelid IN ('distr_table'::regclass,
                          'citus_loc_1'::regclass,
                          'citus_loc_2'::regclass,
                          'citus_loc_3'::regclass,
                          'citus_loc_4'::regclass,
                          'refr_table'::regclass)
    ORDER BY logicalrelid;
ROLLBACK;

SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
  WHERE logicalrelid IN ('distr_table'::regclass,
                         'citus_loc_1'::regclass,
                         'citus_loc_2'::regclass,
                         'citus_loc_3'::regclass,
                         'citus_loc_4'::regclass,
                         'refr_table'::regclass)
  ORDER BY logicalrelid;

BEGIN;
  CREATE TABLE part_citus_loc_1 (a INT UNIQUE) PARTITION BY RANGE (a);
  CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);

  select citus_add_local_table_to_metadata('part_citus_loc_2');

  ALTER TABLE part_citus_loc_1 ADD CONSTRAINT fkey_partitioned_rels FOREIGN KEY (a) references part_citus_loc_2(a);

  SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
    WHERE logicalrelid IN ('part_citus_loc_1'::regclass,
                           'part_citus_loc_2'::regclass)
    ORDER BY logicalrelid;
ROLLBACK;

BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    CREATE TABLE part_citus_loc_2_1 PARTITION OF part_citus_loc_2 FOR VALUES FROM (0) TO (2);

    select citus_add_local_table_to_metadata('part_citus_loc_2');

    ALTER TABLE part_citus_loc_2 ADD CONSTRAINT fkey_partitioned_test FOREIGN KEY (a) references refr_table(a);

    CREATE TABLE part_citus_loc_2_2 PARTITION OF part_citus_loc_2 FOR VALUES FROM (4) TO (5);
    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('part_citus_loc_2'::regclass,
                             'part_citus_loc_2_1'::regclass,
                             'part_citus_loc_2_2'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    CREATE TABLE part_citus_loc_2_1 PARTITION OF part_citus_loc_2 FOR VALUES FROM (0) TO (2);

    select citus_add_local_table_to_metadata('part_citus_loc_2');

    ALTER TABLE part_citus_loc_2 ADD CONSTRAINT fkey_partitioned_test FOREIGN KEY (a) references citus_loc_4(a);

    -- reference to citus local, use alter table attach partition
    CREATE TABLE part_citus_loc_2_2 (a INT UNIQUE);
    ALTER TABLE part_citus_loc_2 ATTACH PARTITION part_citus_loc_2_2 FOR VALUES FROM (3) TO (5);
    CREATE TABLE part_citus_loc_2_3 PARTITION OF part_citus_loc_2 FOR VALUES FROM (7) TO (8);

    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('citus_loc_1'::regclass,
                             'citus_loc_2'::regclass,
                             'citus_loc_3'::regclass,
                             'citus_loc_4'::regclass,
                             'part_citus_loc_2'::regclass,
                             'part_citus_loc_2_1'::regclass,
                             'part_citus_loc_2_2'::regclass,
                             'part_citus_loc_2_3'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

--
-- now mark whole graph as autoConverted = false
--
select citus_add_local_table_to_metadata('citus_loc_1');
SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
  WHERE logicalrelid IN ('distr_table'::regclass,
                         'citus_loc_1'::regclass,
                         'citus_loc_2'::regclass,
                         'citus_loc_3'::regclass,
                         'citus_loc_4'::regclass,
                         'refr_table'::regclass)
  ORDER BY logicalrelid;

BEGIN;
  CREATE TABLE part_citus_loc_1 (a INT UNIQUE REFERENCES citus_loc_1(a)) PARTITION BY RANGE (a);
  SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
    WHERE logicalrelid IN ('distr_table'::regclass,
                          'citus_loc_1'::regclass,
                          'citus_loc_2'::regclass,
                          'citus_loc_3'::regclass,
                          'citus_loc_4'::regclass,
                          'refr_table'::regclass,
                          'part_citus_loc_1'::regclass)
    ORDER BY logicalrelid;
ROLLBACK;

begin;
  CREATE TABLE part_citus_loc_1 (a INT UNIQUE) PARTITION BY RANGE (a);
  ALTER TABLE part_citus_loc_1 ADD CONSTRAINT fkey_testt FOREIGN KEY (a) references citus_loc_3(a);
  SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
    WHERE logicalrelid IN ('distr_table'::regclass,
                          'citus_loc_1'::regclass,
                          'citus_loc_2'::regclass,
                          'citus_loc_3'::regclass,
                          'citus_loc_4'::regclass,
                          'refr_table'::regclass,
                          'part_citus_loc_1'::regclass)
    ORDER BY logicalrelid;
rollback;

CREATE TABLE part_citus_loc_1 (a INT UNIQUE) PARTITION BY RANGE (a);
CREATE TABLE part_citus_loc_1_1 PARTITION OF part_citus_loc_1 FOR VALUES FROM (0) TO (2);
CREATE TABLE part_citus_loc_1_2 PARTITION OF part_citus_loc_1 FOR VALUES FROM (3) TO (5);
ALTER TABLE citus_loc_4 ADD CONSTRAINT fkey_to_part_table FOREIGN KEY (a) REFERENCES part_citus_loc_1(a);

SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
  WHERE logicalrelid IN ('distr_table'::regclass,
                         'citus_loc_1'::regclass,
                         'citus_loc_2'::regclass,
                         'citus_loc_3'::regclass,
                         'citus_loc_4'::regclass,
                         'refr_table'::regclass,
                         'part_citus_loc_1'::regclass,
                         'part_citus_loc_1_1'::regclass,
                         'part_citus_loc_1_2'::regclass)
  ORDER BY logicalrelid;

BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE REFERENCES part_citus_loc_1(a)) PARTITION BY RANGE (a);
    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('distr_table'::regclass,
                            'citus_loc_1'::regclass,
                            'citus_loc_2'::regclass,
                            'citus_loc_3'::regclass,
                            'citus_loc_4'::regclass,
                            'refr_table'::regclass,
                            'part_citus_loc_1'::regclass,
                            'part_citus_loc_1_1'::regclass,
                            'part_citus_loc_1_2'::regclass,
                            'part_citus_loc_2'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

-- use alter table
BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    ALTER TABLE part_citus_loc_2 ADD CONSTRAINT fkey_from_to_partitioned FOREIGN KEY (a) references part_citus_loc_1(a);

    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('distr_table'::regclass,
                            'citus_loc_1'::regclass,
                            'citus_loc_2'::regclass,
                            'citus_loc_3'::regclass,
                            'citus_loc_4'::regclass,
                            'refr_table'::regclass,
                            'part_citus_loc_1'::regclass,
                            'part_citus_loc_1_1'::regclass,
                            'part_citus_loc_1_2'::regclass,
                            'part_citus_loc_2'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

-- alter table foreign key reverse order
BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    ALTER TABLE part_citus_loc_1 ADD CONSTRAINT fkey_from_to_partitioned FOREIGN KEY (a) references part_citus_loc_2(a);

    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('distr_table'::regclass,
                            'citus_loc_1'::regclass,
                            'citus_loc_2'::regclass,
                            'citus_loc_3'::regclass,
                            'citus_loc_4'::regclass,
                            'refr_table'::regclass,
                            'part_citus_loc_1'::regclass,
                            'part_citus_loc_1_1'::regclass,
                            'part_citus_loc_1_2'::regclass,
                            'part_citus_loc_2'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    CREATE TABLE part_citus_loc_2_1 PARTITION OF part_citus_loc_2 FOR VALUES FROM (0) TO (2);

    -- reference to ref
    ALTER TABLE part_citus_loc_2 ADD CONSTRAINT fkey_to_ref_test FOREIGN KEY (a) REFERENCES refr_table(a);

    CREATE TABLE part_citus_loc_2_2 PARTITION OF part_citus_loc_2 FOR VALUES FROM (3) TO (5);

    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('distr_table'::regclass,
                            'citus_loc_1'::regclass,
                            'citus_loc_2'::regclass,
                            'citus_loc_3'::regclass,
                            'citus_loc_4'::regclass,
                            'refr_table'::regclass,
                            'part_citus_loc_1'::regclass,
                            'part_citus_loc_1_1'::regclass,
                            'part_citus_loc_1_2'::regclass,
                            'part_citus_loc_2'::regclass,
                            'part_citus_loc_2_1'::regclass,
                            'part_citus_loc_2_2'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

-- the same, but fkey to citus local, not reference table
-- also with attach partition
BEGIN;
    CREATE TABLE part_citus_loc_2 (a INT UNIQUE) PARTITION BY RANGE (a);
    CREATE TABLE part_citus_loc_2_1 PARTITION OF part_citus_loc_2 FOR VALUES FROM (0) TO (2);

    ALTER TABLE part_citus_loc_2 ADD CONSTRAINT fkey_to_ref_test FOREIGN KEY (a) REFERENCES citus_loc_4(a);

    -- reference to citus local, use create table partition of
    CREATE TABLE part_citus_loc_2_2(a INT UNIQUE);
    ALTER TABLE part_citus_loc_2 ATTACH PARTITION part_citus_loc_2_2 FOR VALUES FROM (3) TO (5);

    CREATE TABLE part_citus_loc_2_3 PARTITION OF part_citus_loc_2 FOR VALUES FROM (7) TO (9);

    SELECT logicalrelid, autoconverted, partmethod FROM pg_dist_partition
      WHERE logicalrelid IN ('distr_table'::regclass,
                            'citus_loc_1'::regclass,
                            'citus_loc_2'::regclass,
                            'citus_loc_3'::regclass,
                            'citus_loc_4'::regclass,
                            'refr_table'::regclass,
                            'part_citus_loc_1'::regclass,
                            'part_citus_loc_1_1'::regclass,
                            'part_citus_loc_1_2'::regclass,
                            'part_citus_loc_2'::regclass,
                            'part_citus_loc_2_1'::regclass,
                            'part_citus_loc_2_2'::regclass,
                            'part_citus_loc_2_3'::regclass)
      ORDER BY logicalrelid;
ROLLBACK;

-- a single drop table cascades into multiple undistributes
DROP TABLE IF EXISTS citus_local_table_1, citus_local_table_2, citus_local_table_3, citus_local_table_2, reference_table_1;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE citus_local_table_1(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE);
CREATE TABLE citus_local_table_2(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE REFERENCES citus_local_table_1(l2));
CREATE TABLE citus_local_table_3(l1 int REFERENCES reference_table_1(r1), l2 int REFERENCES citus_local_table_2(l2));
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass, 'citus_local_table_2'::regclass, 'citus_local_table_3'::regclass) ORDER BY logicalrelid;

-- test DROP OWNED BY

-- Citus does not support "ALTER TABLE OWNER TO" commands. Also, not to deal with tests output
-- difference between community and enterprise, let's disable enable_ddl_propagation here.
SET citus.enable_ddl_propagation to OFF;

CREATE USER another_user;
SELECT run_command_on_workers('CREATE USER another_user');

ALTER TABLE reference_table_1 OWNER TO another_user;
SELECT run_command_on_placements('reference_table_1', 'ALTER TABLE %s OWNER TO another_user');

SET citus.enable_ddl_propagation to ON;

BEGIN;
  DROP OWNED BY another_user cascade;
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ( 'citus_local_table_1'::regclass, 'citus_local_table_2'::regclass, 'citus_local_table_3'::regclass) ORDER BY logicalrelid;
ROLLBACK;

DROP TABLE reference_table_1 CASCADE;
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ( 'citus_local_table_1'::regclass, 'citus_local_table_2'::regclass, 'citus_local_table_3'::regclass) ORDER BY logicalrelid;


-- dropping constraints inside a plpgsql procedure should be fine
DROP TABLE IF EXISTS citus_local_table_1, reference_table_1 CASCADE;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE citus_local_table_1(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;


CREATE OR REPLACE FUNCTION drop_constraint_via_func()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
ALTER TABLE citus_local_table_1 DROP CONSTRAINT citus_local_table_1_l1_fkey;
END;$$;

BEGIN;
  SELECT drop_constraint_via_func();
  SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;
ROLLBACK;

create or replace procedure drop_constraint_via_proc()
language plpgsql
as $$
DECLARE
    res INT := 0;
begin
        ALTER TABLE citus_local_table_1 DROP CONSTRAINT citus_local_table_1_l1_fkey;
    commit;
end;$$;
call drop_constraint_via_proc();

SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;

-- even if the procedure is called from another procedure
DROP TABLE IF EXISTS citus_local_table_1, reference_table_1 CASCADE;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE citus_local_table_1(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;

create or replace procedure drop_constraint_via_proc_top_level()
language plpgsql
as $$
DECLARE
    res INT := 0;
begin
        CALL drop_constraint_via_proc();
    commit;
end;$$;

CALL drop_constraint_via_proc_top_level();
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;


-- even if the procedure is called from an exception handler
DROP TABLE IF EXISTS citus_local_table_1, reference_table_1 CASCADE;
CREATE TABLE reference_table_1(r1 int UNIQUE, r2 int);
SELECT create_reference_table('reference_table_1');
CREATE TABLE citus_local_table_1(l1 int REFERENCES reference_table_1(r1), l2 int UNIQUE);
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;

create or replace procedure drop_constraint_via_proc_exception()
language plpgsql
as $$
DECLARE
    res INT := 0;
begin
		PERFORM 1/0;
    	EXCEPTION
             when others then
        CALL drop_constraint_via_proc();
    commit;
end;$$;

CALL drop_constraint_via_proc_exception();
SELECT logicalrelid, partmethod, repmodel FROM pg_dist_partition WHERE logicalrelid IN ('reference_table_1'::regclass, 'citus_local_table_1'::regclass) ORDER BY logicalrelid;

DROP SCHEMA drop_fkey_cascade CASCADE;
