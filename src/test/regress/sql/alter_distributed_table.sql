SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven;
\gset

CREATE SCHEMA alter_distributed_table;
SET search_path TO alter_distributed_table;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dist_table (a INT, b INT);
SELECT create_distributed_table ('dist_table', 'a', colocate_with := 'none');
INSERT INTO dist_table VALUES (1, 1), (2, 2), (3, 3);

CREATE TABLE colocation_table (a INT, b INT);
SELECT create_distributed_table ('colocation_table', 'a', colocate_with := 'none');

CREATE TABLE colocation_table_2 (a INT, b INT);
SELECT create_distributed_table ('colocation_table_2', 'a', colocate_with := 'none');


SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;

-- test altering distribution column
SELECT alter_distributed_table('dist_table', distribution_column := 'b');
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;

-- test altering shard count
SELECT alter_distributed_table('dist_table', shard_count := 6);
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;

-- test altering colocation, note that shard count will also change
SELECT alter_distributed_table('dist_table', colocate_with := 'alter_distributed_table.colocation_table');
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;

-- test altering shard count with cascading, note that the colocation will be kept
SELECT alter_distributed_table('dist_table', shard_count := 8, cascade_to_colocated := true);
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;

-- test altering shard count without cascading, note that the colocation will be broken
SELECT alter_distributed_table('dist_table', shard_count := 10, cascade_to_colocated := false);
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2');
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables
    WHERE table_name IN ('dist_table', 'colocation_table', 'colocation_table_2') GROUP BY colocation_id ORDER BY 1;


-- test partitions
CREATE TABLE partitioned_table (id INT, a INT) PARTITION BY RANGE (id);
SELECT create_distributed_table('partitioned_table', 'id', colocate_with := 'none');
CREATE TABLE partitioned_table_1_5 PARTITION OF partitioned_table FOR VALUES FROM (1) TO (5);
CREATE TABLE partitioned_table_6_10 PARTITION OF partitioned_table FOR VALUES FROM (6) TO (10);
INSERT INTO partitioned_table VALUES (2, 12), (7, 2);

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT table_name::text, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;

-- test altering the parent table
SELECT alter_distributed_table('partitioned_table', shard_count := 10, distribution_column := 'a');

-- test altering the partition
SELECT alter_distributed_table('partitioned_table_1_5', shard_count := 10, distribution_column := 'a');

SELECT logicalrelid::text FROM pg_dist_partition WHERE logicalrelid::regclass::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT run_command_on_workers($$SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relname LIKE 'partitioned\_table%'$$);
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'partitioned_table'::regclass ORDER BY 1;
SELECT table_name::text, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'partitioned\_table%' ORDER BY 1;
SELECT * FROM partitioned_table ORDER BY 1, 2;
SELECT * FROM partitioned_table_1_5 ORDER BY 1, 2;
SELECT * FROM partitioned_table_6_10 ORDER BY 1, 2;


-- test references
CREATE TABLE referenced_dist_table (a INT UNIQUE);
CREATE TABLE referenced_ref_table (a INT UNIQUE);
CREATE TABLE table_with_references (a1 INT UNIQUE REFERENCES referenced_dist_table(a), a2 INT REFERENCES referenced_ref_table(a));
CREATE TABLE referencing_dist_table (a INT REFERENCES table_with_references(a1));

SELECT create_distributed_table('referenced_dist_table', 'a', colocate_with:='none');
SELECT create_reference_table('referenced_ref_table');
SELECT create_distributed_table('table_with_references', 'a1', colocate_with:='referenced_dist_table');
SELECT create_distributed_table('referencing_dist_table', 'a', colocate_with:='referenced_dist_table');

SET client_min_messages TO WARNING;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1;
SELECT alter_distributed_table('table_with_references', shard_count := 12, cascade_to_colocated := true);
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1;
SELECT alter_distributed_table('table_with_references', shard_count := 10, cascade_to_colocated := false);
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1;


-- check when multi shard modify mode is set to sequential
SELECT alter_distributed_table('referenced_dist_table', colocate_with:='none');
CREATE TABLE ref_to_dist_table(a INT REFERENCES referenced_dist_table(a));
CREATE TABLE ref_to_ref_table(a INT REFERENCES referenced_ref_table(a));
SELECT create_distributed_table('ref_to_dist_table', 'a', colocate_with:='referenced_dist_table');
SELECT create_distributed_table('ref_to_ref_table', 'a', colocate_with:='none');

-- alter a table referencing a reference table
SELECT alter_distributed_table('ref_to_ref_table', shard_count:=6);

-- let's create a table that is not colocated with a table that references a reference table
CREATE TABLE col_with_ref_to_dist (a INT);
SELECT create_distributed_table('col_with_ref_to_dist', 'a', colocate_with:='ref_to_dist_table');
-- and create a table colocated with a table that references a reference table
CREATE TABLE col_with_ref_to_ref (a INT);
SELECT alter_distributed_table('ref_to_ref_table', colocate_with:='none');
SELECT create_distributed_table('col_with_ref_to_ref', 'a', colocate_with:='ref_to_ref_table');

-- alter a table colocated with a table referencing a reference table with cascading
SELECT alter_distributed_table('col_with_ref_to_ref', shard_count:=8, cascade_to_colocated:=true);
-- alter a table colocated with a table referencing a reference table without cascading
SELECT alter_distributed_table('col_with_ref_to_ref', shard_count:=10, cascade_to_colocated:=false);
-- alter a table not colocated with a table referencing a reference table with cascading
SELECT alter_distributed_table('col_with_ref_to_dist', shard_count:=6, cascade_to_colocated:=true);


\if :server_version_above_eleven
-- test altering columnar table
CREATE TABLE columnar_table (a INT) USING columnar;
SELECT create_distributed_table('columnar_table', 'a', colocate_with:='none');
SELECT table_name::text, shard_count, access_method FROM public.citus_tables WHERE table_name::text = 'columnar_table';
SELECT alter_distributed_table('columnar_table', shard_count:=6);
SELECT table_name::text, shard_count, access_method FROM public.citus_tables WHERE table_name::text = 'columnar_table';
\endif


-- test with metadata sync
SET citus.replication_model TO 'streaming';
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

CREATE TABLE metadata_sync_table (a BIGSERIAL);
SELECT create_distributed_table('metadata_sync_table', 'a', colocate_with:='none');

SELECT alter_distributed_table('metadata_sync_table', shard_count:=6);
SELECT alter_distributed_table('metadata_sync_table', shard_count:=8);

SELECT table_name, shard_count FROM public.citus_tables WHERE table_name::text = 'metadata_sync_table';

SET citus.replication_model TO DEFAULT;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

-- test complex cascade operations
CREATE TABLE cas_1 (a INT UNIQUE);
CREATE TABLE cas_2 (a INT UNIQUE);

CREATE TABLE cas_3 (a INT UNIQUE);
CREATE TABLE cas_4 (a INT UNIQUE);

CREATE TABLE cas_par (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE cas_par_1 PARTITION OF cas_par FOR VALUES FROM (1) TO (4);
CREATE TABLE cas_par_2 PARTITION OF cas_par FOR VALUES FROM (5) TO (8);

CREATE TABLE cas_col (a INT UNIQUE);

-- add foreign keys from and to partitions
ALTER TABLE cas_par_1 ADD CONSTRAINT fkey_from_par_1 FOREIGN KEY (a) REFERENCES cas_1(a);
ALTER TABLE cas_2 ADD CONSTRAINT fkey_to_par_1 FOREIGN KEY (a) REFERENCES cas_par_1(a);

ALTER TABLE cas_par ADD CONSTRAINT fkey_from_par FOREIGN KEY (a) REFERENCES cas_3(a);
ALTER TABLE cas_4 ADD CONSTRAINT fkey_to_par FOREIGN KEY (a) REFERENCES cas_par(a);

-- distribute all the tables
SELECT create_distributed_table('cas_1', 'a', colocate_with:='none');
SELECT create_distributed_table('cas_3', 'a', colocate_with:='cas_1');
SELECT create_distributed_table('cas_par', 'a', colocate_with:='cas_1');
SELECT create_distributed_table('cas_2', 'a', colocate_with:='cas_1');
SELECT create_distributed_table('cas_4', 'a', colocate_with:='cas_1');
SELECT create_distributed_table('cas_col', 'a', colocate_with:='cas_1');

SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'cas_par_1' OR confrelid::regclass::text = 'cas_par_1') ORDER BY 1, 2;
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'cas_par'::regclass ORDER BY 1;

-- alter the cas_col and cascade the change
SELECT alter_distributed_table('cas_col', shard_count:=6, cascade_to_colocated:=true);

SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'cas_par_1' OR confrelid::regclass::text = 'cas_par_1') ORDER BY 1, 2;
SELECT inhrelid::regclass::text FROM pg_catalog.pg_inherits WHERE inhparent = 'cas_par'::regclass ORDER BY 1;
SET client_min_messages TO DEFAULT;


-- test changing dist column and colocating partitioned table without changing shard count
CREATE TABLE col_table (a INT);
SELECT create_distributed_table('col_table', 'a', colocate_with:='none');
CREATE TABLE par_table (a BIGINT, b INT) PARTITION BY RANGE (a);
SELECT create_distributed_table('par_table', 'a', colocate_with:='none');
CREATE TABLE par_table_1 (a BIGINT, b INT);
SELECT create_distributed_table('par_table_1', 'a', colocate_with:='par_table');
ALTER TABLE par_table ATTACH PARTITION par_table_1 FOR VALUES FROM (1) TO (5);

SELECT alter_distributed_table('par_table', distribution_column:='b', colocate_with:='col_table');

-- test messages
-- test nothing to change
SELECT alter_distributed_table('dist_table');
SELECT alter_distributed_table('dist_table', cascade_to_colocated := false);

-- no operation UDF calls
SELECT alter_distributed_table('dist_table', distribution_column := 'b');
SELECT alter_distributed_table('dist_table', shard_count := 10);
-- first colocate the tables, then try to re-colococate
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table');
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table');

-- test some changes while keeping others same
-- shouldn't error but should have notices about no-change parameters
SELECT alter_distributed_table('dist_table', distribution_column:='b', shard_count:=4, cascade_to_colocated:=false);
SELECT alter_distributed_table('dist_table', shard_count:=4, colocate_with:='colocation_table_2');
SELECT alter_distributed_table('dist_table', colocate_with:='colocation_table_2', distribution_column:='a');

-- test cascading distribution column, should error
SELECT alter_distributed_table('dist_table', distribution_column := 'b', cascade_to_colocated := true);
SELECT alter_distributed_table('dist_table', distribution_column := 'b', shard_count:=12, colocate_with:='colocation_table_2', cascade_to_colocated := true);

-- test nothing to cascade
SELECT alter_distributed_table('dist_table', cascade_to_colocated := true);

-- test cascading colocate_with := 'none'
SELECT alter_distributed_table('dist_table', colocate_with := 'none', cascade_to_colocated := true);

-- test changing shard count of a colocated table without cascade_to_colocated, should error
SELECT alter_distributed_table('dist_table', shard_count := 14);

-- test changing shard count of a non-colocated table without cascade_to_colocated, shouldn't error
SELECT alter_distributed_table('dist_table', colocate_with := 'none');
SELECT alter_distributed_table('dist_table', shard_count := 14);

-- test altering a table into colocating with a table but giving a different shard count
SELECT alter_distributed_table('dist_table', colocate_with := 'colocation_table', shard_count := 16);

-- test colocation with distribution columns with different data types
CREATE TABLE different_type_table (a TEXT);
SELECT create_distributed_table('different_type_table', 'a');

SELECT alter_distributed_table('dist_table', colocate_with := 'different_type_table');
SELECT alter_distributed_table('dist_table', distribution_column := 'a', colocate_with := 'different_type_table');

-- test shard_count := 0
SELECT alter_distributed_table('dist_table', shard_count := 0);

-- test colocating with non-distributed table
CREATE TABLE reference_table (a INT);
SELECT create_reference_table('reference_table');
SELECT alter_distributed_table('dist_table', colocate_with:='reference_table');

-- test append table
CREATE TABLE append_table (a INT);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT alter_distributed_table('append_table', shard_count:=6);

-- test keeping dependent materialized views
CREATE TABLE mat_view_test (a int, b int);
SELECT create_distributed_table('mat_view_test', 'a');
INSERT INTO mat_view_test VALUES (1,1), (2,2);
CREATE MATERIALIZED VIEW mat_view AS SELECT * FROM mat_view_test;
SELECT alter_distributed_table('mat_view_test', shard_count := 5, cascade_to_colocated := false);
SELECT * FROM mat_view ORDER BY a;

SET client_min_messages TO WARNING;
DROP SCHEMA alter_distributed_table CASCADE;
