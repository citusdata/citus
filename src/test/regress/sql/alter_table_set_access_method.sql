--
-- ALTER_TABLE_SET_ACCESS_METHOD
--

CREATE TABLE alter_am_pg_version_table (a INT);
SELECT alter_table_set_access_method('alter_am_pg_version_table', 'columnar');
DROP TABLE alter_am_pg_version_table;


CREATE SCHEMA alter_table_set_access_method;
SET search_path TO alter_table_set_access_method;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

SELECT public.run_command_on_coordinator_and_workers($Q$
	CREATE FUNCTION fake_am_handler(internal)
	RETURNS table_am_handler
	AS 'citus'
	LANGUAGE C;
	CREATE ACCESS METHOD fake_am TYPE TABLE HANDLER fake_am_handler;
$Q$);

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a');
INSERT INTO dist_table VALUES (1, 1), (2, 2), (3, 3);

SELECT table_name, access_method FROM public.citus_tables WHERE table_name::text = 'dist_table' ORDER BY 1;
SELECT alter_table_set_access_method('dist_table', 'columnar');
SELECT table_name, access_method FROM public.citus_tables WHERE table_name::text = 'dist_table' ORDER BY 1;


-- test partitions
CREATE TABLE partitioned_table (id INT, a INT) PARTITION BY RANGE (id);
CREATE TABLE partitioned_table_1_5 PARTITION OF partitioned_table FOR VALUES FROM (1) TO (5);
CREATE TABLE partitioned_table_6_10 PARTITION OF partitioned_table FOR VALUES FROM (6) TO (10);
SELECT create_distributed_table('partitioned_table', 'id');
INSERT INTO partitioned_table VALUES (2, 12), (7, 2);

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT table_name::text, access_method FROM public.citus_tables WHERE table_name::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

-- altering partitioned tables' access methods is not supported
SELECT alter_table_set_access_method('partitioned_table', 'columnar');
-- test altering the partition's access method
SELECT alter_table_set_access_method('partitioned_table_1_5', 'columnar');

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT table_name::text, access_method FROM public.citus_tables WHERE table_name::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

-- try to compress partitions with an integer partition column
CALL alter_old_partitions_set_access_method('partitioned_table', '2021-01-01', 'columnar');

CREATE TABLE time_partitioned (event_time timestamp, event int) partition by range (event_time);
SELECT create_distributed_table('time_partitioned', 'event_time');
CREATE TABLE time_partitioned_d00 PARTITION OF time_partitioned FOR VALUES FROM ('2000-01-01') TO ('2009-12-31');
CREATE TABLE time_partitioned_d10 PARTITION OF time_partitioned FOR VALUES FROM ('2010-01-01') TO ('2019-12-31');
CREATE TABLE time_partitioned_d20 PARTITION OF time_partitioned FOR VALUES FROM ('2020-01-01') TO ('2029-12-31');
INSERT INTO time_partitioned VALUES ('2005-01-01', 1);
INSERT INTO time_partitioned VALUES ('2015-01-01', 2);
INSERT INTO time_partitioned VALUES ('2025-01-01', 3);

\set VERBOSITY terse

-- compress no partitions
CALL alter_old_partitions_set_access_method('time_partitioned', '1999-01-01', 'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'time_partitioned'::regclass ORDER BY partition::text;
SELECT event FROM time_partitioned ORDER BY 1;

-- compress 2 old partitions
CALL alter_old_partitions_set_access_method('time_partitioned', '2021-01-01', 'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'time_partitioned'::regclass ORDER BY partition::text;
SELECT event FROM time_partitioned ORDER BY 1;

-- will not be compressed again
CALL alter_old_partitions_set_access_method('time_partitioned', '2021-01-01', 'columnar');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'time_partitioned'::regclass ORDER BY partition::text;
SELECT event FROM time_partitioned ORDER BY 1;

-- back to heap
CALL alter_old_partitions_set_access_method('time_partitioned', '2021-01-01', 'heap');
SELECT partition, access_method FROM time_partitions WHERE parent_table = 'time_partitioned'::regclass ORDER BY partition::text;
SELECT event FROM time_partitioned ORDER BY 1;

\set VERBOSITY default

DROP TABLE time_partitioned;

-- test altering a table with index to columnar
CREATE TABLE index_table (a INT) USING heap;
CREATE INDEX idx1 ON index_table (a);
-- also create an index with statistics
CREATE INDEX idx2 ON index_table ((a+1));
ALTER INDEX idx2 ALTER COLUMN 1 SET STATISTICS 300;
SELECT indexname FROM pg_indexes WHERE schemaname = 'alter_table_set_access_method' AND tablename = 'index_table' order by indexname;
SELECT a.amname FROM pg_class c, pg_am a where c.relname = 'index_table' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;
SELECT alter_table_set_access_method('index_table', 'columnar');
SELECT indexname FROM pg_indexes WHERE schemaname = 'alter_table_set_access_method' AND tablename = 'index_table' order by indexname;
SELECT a.amname FROM pg_class c, pg_am a where c.relname = 'index_table' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;

CREATE TABLE "heap_\'tbl" (
  c1 CIRCLE,
  c2 TEXT,
  i int4[],
  p point,
  a int,
  EXCLUDE USING gist
    (c1 WITH &&, (c2::circle) WITH &&)
    WHERE (circle_center(c1) <> '(0,0)'),
  EXCLUDE USING btree
    (a WITH =)
	INCLUDE(p)
	WHERE (c2 < 'astring')
);

CREATE INDEX heap_tbl_gin ON "heap_\'tbl" USING gin (i);
CREATE INDEX "heap_tbl_\'gist" ON "heap_\'tbl" USING gist(p);
CREATE INDEX heap_tbl_brin ON "heap_\'tbl" USING brin (a) WITH (pages_per_range = 1);

CREATE INDEX heap_tbl_hash ON "heap_\'tbl" USING hash (c2);
ALTER TABLE "heap_\'tbl" ADD CONSTRAINT heap_tbl_unique UNIQUE (c2);

CREATE UNIQUE INDEX "heap_tbl_\'btree" ON "heap_\'tbl" USING btree (a);
ALTER TABLE "heap_\'tbl" ADD CONSTRAINT "heap_tbl_\'pkey" PRIMARY KEY USING INDEX "heap_tbl_\'btree";

SELECT indexname FROM pg_indexes
WHERE schemaname = 'alter_table_set_access_method' AND
      tablename = 'heap_\''tbl'
ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = 'heap_\''tbl'::regclass
ORDER BY conname;

SELECT alter_table_set_access_method('heap_\''tbl', 'columnar');

SELECT indexname FROM pg_indexes
WHERE schemaname = 'alter_table_set_access_method' AND
      tablename = 'heap_\''tbl'
ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = 'heap_\''tbl'::regclass
ORDER BY conname;

-- test different table types
SET client_min_messages to WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId := 0);
SET client_min_messages to DEFAULT;
CREATE TABLE table_type_dist (a INT);
SELECT create_distributed_table('table_type_dist', 'a');
CREATE TABLE table_type_ref (a INT);
SELECT create_reference_table('table_type_ref');
CREATE TABLE table_type_citus_local(a INT);
SELECT citus_add_local_table_to_metadata('table_type_citus_local');
CREATE TABLE table_type_pg_local (a INT);

SELECT table_name, citus_table_type, distribution_column, shard_count, access_method FROM public.citus_tables WHERE table_name::text LIKE 'table\_type%' ORDER BY 1;
SELECT c.relname, a.amname FROM pg_class c, pg_am a where c.relname SIMILAR TO 'table_type\D*' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;

SELECT alter_table_set_access_method('table_type_dist', 'columnar');
SELECT alter_table_set_access_method('table_type_ref', 'columnar');
SELECT alter_table_set_access_method('table_type_pg_local', 'columnar');
SELECT alter_table_set_access_method('table_type_citus_local', 'columnar');

SELECT table_name, citus_table_type, distribution_column, shard_count, access_method FROM public.citus_tables WHERE table_name::text LIKE 'table\_type%' ORDER BY 1;
SELECT c.relname, a.amname FROM pg_class c, pg_am a where c.relname SIMILAR TO 'table_type\D*' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;

-- test when the parent of a partition has foreign key to a reference table
create table test_pk(n int primary key);
create table test_fk_p(i int references test_pk(n)) partition by range(i);
create table test_fk_p0 partition of test_fk_p for values from (0) to (10);
create table test_fk_p1 partition of test_fk_p for values from (10) to (20);
select alter_table_set_access_method('test_fk_p1', 'columnar');

-- test changing into same access method
CREATE TABLE same_access_method (a INT);
SELECT alter_table_set_access_method('same_access_method', 'heap');

-- test keeping dependent materialized views
CREATE TABLE mat_view_test (a int);
INSERT INTO mat_view_test VALUES (1), (2);
CREATE MATERIALIZED VIEW mat_view AS SELECT * FROM mat_view_test;
SELECT alter_table_set_access_method('mat_view_test','columnar');
SELECT * FROM mat_view ORDER BY a;

CREATE SEQUENCE c_seq;
CREATE TABLE local(a int, b bigserial, c int default nextval('c_seq'));
INSERT INTO local VALUES (3);
create materialized view m_local as select * from local;
create view v_local as select * from local;


CREATE TABLE ref(a int);
SELECT create_Reference_table('ref');
INSERT INTO ref VALUES (4),(5);
create materialized view m_ref as select * from ref;
create view v_ref as select * from ref;


CREATE TABLE dist(a int);
SELECT create_distributed_table('dist', 'a');
INSERT INTO dist VALUES (7),(9);
create materialized view m_dist as select * from dist;
create view v_dist as select * from dist;

\set VERBOSITY terse

select alter_table_set_access_method('local','columnar');
select alter_table_set_access_method('ref','columnar');
select alter_table_set_access_method('dist','columnar');

SELECT alter_distributed_table('dist', shard_count:=1, cascade_to_colocated:=false);

select alter_table_set_access_method('local','heap');
select alter_table_set_access_method('ref','heap');
select alter_table_set_access_method('dist','heap');

\set VERBOSITY default

SELECT * FROM m_local;
SELECT * FROM m_ref;
SELECT * FROM m_dist;


SELECT * FROM v_local;
SELECT * FROM v_ref;
SELECT * FROM v_dist;

SELECT relname, relkind
	FROM pg_class
	WHERE relname IN (
		'v_dist',
		'v_ref',
		'v_local',
		'm_dist',
		'm_ref',
		'm_local'
	)
	ORDER BY relname ASC;

-- test long table names
SET client_min_messages TO DEBUG1;
CREATE TABLE abcde_0123456789012345678901234567890123456789012345678901234567890123456789 (x int, y int);
SELECT create_distributed_table('abcde_0123456789012345678901234567890123456789012345678901234567890123456789', 'x');
SELECT alter_table_set_access_method('abcde_0123456789012345678901234567890123456789012345678901234567890123456789', 'columnar');
SELECT * FROM abcde_0123456789012345678901234567890123456789012345678901234567890123456789;
RESET client_min_messages;

create table events (event_id bigserial, event_time timestamptz default now(), payload text);
create index on events (event_id);
insert into events (payload) select 'hello-'||s from generate_series(1,10) s;

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
\gset

BEGIN;
  \if :server_version_ge_16
  SET LOCAL debug_parallel_query = regress;
  \else
  SET LOCAL force_parallel_mode = regress;
  \endif
  SET LOCAL min_parallel_table_scan_size = 1;
  SET LOCAL parallel_tuple_cost = 0;
  SET LOCAL max_parallel_workers = 4;
  SET LOCAL max_parallel_workers_per_gather = 4;
  select alter_table_set_access_method('events', 'columnar');
COMMIT;

--create the view to test alter table set access method on it
CREATE TABLE view_test_table (id int, val int, flag bool, kind int);
SELECT create_distributed_table('view_test_table','id');
INSERT INTO view_test_table VALUES (1, 1, true, 99), (2, 2, false, 99), (2, 3, true, 88);
CREATE VIEW view_test_view AS SELECT * FROM view_test_table;

-- error out when attempting to set access method of a view.
select alter_table_set_access_method('view_test_view','columnar');

SET client_min_messages TO WARNING;
DROP SCHEMA alter_table_set_access_method CASCADE;
