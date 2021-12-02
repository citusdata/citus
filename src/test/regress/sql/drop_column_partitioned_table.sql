CREATE SCHEMA drop_column_partitioned_table;
SET search_path TO drop_column_partitioned_table;

SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 2580000;

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

-- prepared statements should work fine even after columns are dropped
PREPARE drop_col_prepare_insert(int, date, jsonb) AS INSERT INTO sensors (measureid, eventdatetime, measure_data) VALUES ($1, $2, $3);
PREPARE drop_col_prepare_select(int, date) AS SELECT count(*) FROM sensors WHERE measureid = $1 AND eventdatetime = $2;

-- execute 7 times to make sure it is cached
EXECUTE drop_col_prepare_insert(1, '2000-10-01', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-02', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-03', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-04', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-05', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-06', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(1, '2000-10-07', row_to_json(row(1)));
EXECUTE drop_col_prepare_select(1, '2000-10-01');
EXECUTE drop_col_prepare_select(1, '2000-10-02');
EXECUTE drop_col_prepare_select(1, '2000-10-03');
EXECUTE drop_col_prepare_select(1, '2000-10-04');
EXECUTE drop_col_prepare_select(1, '2000-10-05');
EXECUTE drop_col_prepare_select(1, '2000-10-06');
EXECUTE drop_col_prepare_select(1, '2000-10-07');

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

-- show that all partitions have the same distribution key
SELECT
	p.logicalrelid::regclass, column_to_column_name(p.logicalrelid, p.partkey)
FROM
	pg_dist_partition p
WHERE
	logicalrelid IN ('sensors'::regclass, 'sensors_2000'::regclass,
					 'sensors_2001'::regclass, 'sensors_2002'::regclass,
					 'sensors_2003'::regclass, 'sensors_2004'::regclass)
ORDER BY 1,2;

-- show that all the tables prune to the same shard for the same distribution key
WITH
	sensors_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors', 3)),
	sensors_2000_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors_2000', 3)),
	sensors_2001_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors_2001', 3)),
	sensors_2002_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors_2002', 3)),
	sensors_2003_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors_2003', 3)),
	sensors_2004_shardid AS (SELECT * FROM get_shard_id_for_distribution_column('sensors_2004', 3)),
	all_shardids AS (SELECT * FROM sensors_shardid UNION SELECT * FROM sensors_2000_shardid UNION
					 SELECT * FROM sensors_2001_shardid UNION SELECT * FROM sensors_2002_shardid
					 UNION SELECT * FROM sensors_2003_shardid UNION SELECT * FROM sensors_2004_shardid)
SELECT logicalrelid, shardid, shardminvalue, shardmaxvalue FROM pg_dist_shard WHERE shardid IN (SELECT * FROM all_shardids) ORDER BY 1,2,3,4;

VACUUM ANALYZE sensors, sensors_2000, sensors_2001, sensors_2002, sensors_2003;

-- show that both INSERT and SELECT can route to a single node when distribution
-- key is provided in the query
EXPLAIN (COSTS FALSE) INSERT INTO sensors VALUES (3, '2000-02-02', row_to_json(row(1)));
EXPLAIN (COSTS FALSE) INSERT INTO sensors_2000 VALUES (3, '2000-01-01', row_to_json(row(1)));
EXPLAIN (COSTS FALSE) INSERT INTO sensors_2001 VALUES (3, '2001-01-01', row_to_json(row(1)));
EXPLAIN (COSTS FALSE) INSERT INTO sensors_2002 VALUES (3, '2002-01-01', row_to_json(row(1)));
EXPLAIN (COSTS FALSE) INSERT INTO sensors_2003 VALUES (3, '2003-01-01', row_to_json(row(1)));

SELECT public.explain_has_single_task(
	$$
EXPLAIN (COSTS FALSE) SELECT count(*) FROM sensors WHERE measureid = 3 AND eventdatetime = '2000-02-02';
	$$
);

SELECT public.explain_has_single_task(
	$$
EXPLAIN (COSTS FALSE) SELECT count(*) FROM sensors_2003 WHERE measureid = 3;
	$$
);

SELECT public.explain_has_single_task(
	$$
EXPLAIN (COSTS FALSE) SELECT count(*) FROM sensors_2000 WHERE measureid = 3;
	$$
);

SELECT public.explain_has_single_task(
	$$
EXPLAIN (COSTS FALSE) SELECT count(*) FROM sensors_2001 WHERE measureid = 3;
	$$
);

SELECT public.explain_has_single_task(
	$$
EXPLAIN (COSTS FALSE) SELECT count(*) FROM sensors_2002 WHERE measureid = 3;
	$$
);


-- execute 7 times to make sure it is re-cached
EXECUTE drop_col_prepare_insert(3, '2000-10-01', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(3, '2001-10-01', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(3, '2002-10-01', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(3, '2003-10-01', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(3, '2003-10-02', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(4, '2003-10-03', row_to_json(row(1)));
EXECUTE drop_col_prepare_insert(5, '2003-10-04', row_to_json(row(1)));
EXECUTE drop_col_prepare_select(3, '2000-10-01');
EXECUTE drop_col_prepare_select(3, '2001-10-01');
EXECUTE drop_col_prepare_select(3, '2002-10-01');
EXECUTE drop_col_prepare_select(3, '2003-10-01');
EXECUTE drop_col_prepare_select(3, '2003-10-02');
EXECUTE drop_col_prepare_select(4, '2003-10-03');
EXECUTE drop_col_prepare_select(5, '2003-10-04');

-- non-fast router planner queries should also work
-- so we switched to DEBUG2 to show that dist. key
-- and the query is router
SET client_min_messages TO DEBUG2;
SELECT count(*) FROM (
SELECT * FROM sensors WHERE measureid = 3
	UNION
SELECT * FROM sensors_2000 WHERE measureid = 3
	UNION
SELECT * FROM sensors_2001 WHERE measureid = 3
	UNION
SELECT * FROM sensors_2002 WHERE measureid = 3
	UNION
SELECT * FROM sensors_2003 WHERE measureid = 3
	UNION
SELECT * FROM sensors_2004 WHERE measureid = 3
) as foo;

RESET client_min_messages;

-- show that all partitions have the same distribution key
-- even after alter_distributed_table changes the shard count

-- remove this comment once https://github.com/citusdata/citus/issues/5137 is fixed
--SELECT alter_distributed_table('sensors', shard_count:='3');

SELECT
	p.logicalrelid::regclass, column_to_column_name(p.logicalrelid, p.partkey)
FROM
	pg_dist_partition p
WHERE
	logicalrelid IN ('sensors'::regclass, 'sensors_2000'::regclass,
					 'sensors_2001'::regclass, 'sensors_2002'::regclass,
					 'sensors_2003'::regclass, 'sensors_2004'::regclass)
ORDER BY 1,2;

\c - - - :worker_1_port
SET search_path TO drop_column_partitioned_table;
SELECT
	p.logicalrelid::regclass, column_to_column_name(p.logicalrelid, p.partkey)
FROM
	pg_dist_partition p
WHERE
	logicalrelid IN ('sensors'::regclass, 'sensors_2000'::regclass,
					 'sensors_2001'::regclass, 'sensors_2002'::regclass,
					 'sensors_2003'::regclass, 'sensors_2004'::regclass)
ORDER BY 1,2;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA drop_column_partitioned_table CASCADE;
