-- test for Postgres version
-- should error before PG12
CREATE TABLE alter_am_pg_version_table (a INT);
SELECT alter_table_set_access_method('alter_am_pg_version_table', 'columnar');
DROP TABLE alter_am_pg_version_table;

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven
\gset
\if :server_version_above_eleven
\else
\q
\endif


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
-- the index will be dropped
CREATE TABLE index_table (a INT) USING heap;
CREATE INDEX idx1 ON index_table (a);
-- also create an index with statistics
CREATE INDEX idx2 ON index_table ((a+1));
ALTER INDEX idx2 ALTER COLUMN 1 SET STATISTICS 300;
SELECT indexname FROM pg_indexes WHERE schemaname = 'alter_table_set_access_method' AND tablename = 'index_table';
SELECT a.amname FROM pg_class c, pg_am a where c.relname = 'index_table' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;
SELECT alter_table_set_access_method('index_table', 'columnar');
SELECT indexname FROM pg_indexes WHERE schemaname = 'alter_table_set_access_method' AND tablename = 'index_table';
SELECT a.amname FROM pg_class c, pg_am a where c.relname = 'index_table' AND c.relnamespace = 'alter_table_set_access_method'::regnamespace AND c.relam = a.oid;


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

SELECT alter_table_set_access_method('table_type_dist', 'fake_am');
SELECT alter_table_set_access_method('table_type_ref', 'fake_am');
SELECT alter_table_set_access_method('table_type_pg_local', 'fake_am');
SELECT alter_table_set_access_method('table_type_citus_local', 'fake_am');

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

CREATE TABLE local(a int);
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


select alter_table_set_access_method('local','columnar');
select alter_table_set_access_method('ref','columnar');
select alter_table_set_access_method('dist','columnar');


SELECT alter_distributed_table('dist', shard_count:=1, cascade_to_colocated:=false);

select alter_table_set_access_method('local','heap');
select alter_table_set_access_method('ref','heap');
select alter_table_set_access_method('dist','heap');

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

SET client_min_messages TO WARNING;
DROP SCHEMA alter_table_set_access_method CASCADE;
SELECT 1 FROM master_remove_node('localhost', :master_port);
