CREATE SCHEMA local_shard_execution_dropped_column;
SET search_path TO local_shard_execution_dropped_column;
GRANT ALL ON SCHEMA local_shard_execution_dropped_column TO regularuser;

CREATE TABLE t1 (a int, b int, c int UNIQUE, d int, e int);
ALTER TABLE t1 DROP COLUMN e;
SELECT create_distributed_table('t1', 'c');
ALTER TABLE t1 DROP COLUMN b;
ALTER TABLE t1 DROP COLUMN d;


-- create a partitioned table with some columns that
-- are going to be dropped within the tests
CREATE TABLE sensors(
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

-- drop column even before attaching any partitions
ALTER TABLE sensors DROP COLUMN col_to_drop_1;

-- now attach the first partition and create the distributed table
CREATE TABLE sensors_2000 PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2001-01-01');
SELECT create_distributed_table('sensors', 'measureid');

-- drop another column before attaching another partition
-- with .. PARTITION OF .. syntax
ALTER TABLE sensors DROP COLUMN col_to_drop_0;
CREATE TABLE sensors_2001 PARTITION OF sensors FOR VALUES FROM ('2001-01-01') TO ('2002-01-01');

-- drop another column before attaching another partition
-- with ALTER TABLE .. ATTACH PARTITION
ALTER TABLE sensors DROP COLUMN col_to_drop_2;

CREATE TABLE sensors_2002(
col_to_drop_4 date, col_to_drop_3 inet, measureid integer, eventdatetime date, measure_data jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data));
ALTER TABLE sensors ATTACH PARTITION sensors_2002 FOR VALUES FROM ('2002-01-01') TO ('2003-01-01');

-- drop another column before attaching another partition
-- that is already distributed
ALTER TABLE sensors DROP COLUMN col_to_drop_3;

CREATE TABLE sensors_2003(
col_to_drop_4 date, measureid integer, eventdatetime date, measure_data jsonb,
PRIMARY KEY (measureid, eventdatetime, measure_data));

SELECT create_distributed_table('sensors_2003', 'measureid');
ALTER TABLE sensors ATTACH PARTITION sensors_2003 FOR VALUES FROM ('2003-01-01') TO ('2004-01-01');

CREATE TABLE sensors_2004(
col_to_drop_4 date, measureid integer NOT NULL, eventdatetime date NOT NULL, measure_data jsonb NOT NULL);

ALTER TABLE sensors ATTACH PARTITION sensors_2004 FOR VALUES FROM ('2004-01-01') TO ('2005-01-01');
ALTER TABLE sensors DROP COLUMN col_to_drop_4;
SELECT alter_table_set_access_method('sensors_2004', 'columnar');
