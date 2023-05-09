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

-- right now dist_table has columns a, b, dist_column is b, it has 6 shards
-- column cache is: a pos 1, b pos 2

-- let's add another column
ALTER TABLE dist_table ADD COLUMN c int DEFAULT 1;

-- right now column cache is: a pos 1, b pos 2, c pos 3

-- test using alter_distributed_table to change shard count after dropping one column
ALTER TABLE dist_table DROP COLUMN a;

-- right now column cache is: a pos 1 attisdropped=true, b pos 2, c pos 3

-- let's try changing the shard count
SELECT alter_distributed_table('dist_table', shard_count := 7, cascade_to_colocated := false);

-- right now column cache is: b pos 1, c pos 2 because a new table has been created
-- check that b is still distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

-- let's add another column
ALTER TABLE dist_table ADD COLUMN d int DEFAULT 2;

-- right now column cache is: b pos 1, c pos 2, d pos 3, dist_column is b

-- test using alter_distributed_table to change dist. column after dropping one column
ALTER TABLE dist_table DROP COLUMN c;

-- right now column cache is: b pos 1, c pos 2 attisdropped=true, d pos 3

-- let's try changing the distribution column
SELECT alter_distributed_table('dist_table', distribution_column := 'd');

-- right now column cache is: b pos 1, d pos 2 because a new table has been created
-- check that d is the distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

-- add another column and undistribute
ALTER TABLE dist_table ADD COLUMN e int DEFAULT 3;
SELECT undistribute_table('dist_table');

-- right now column cache is: b pos 1, d pos 2, e pos 3, table is not Citus table
-- try dropping column and then distributing

ALTER TABLE dist_table DROP COLUMN b;

-- right now column cache is: b pos 1 attisdropped=true, d pos 2, e pos 3

-- distribute with d
SELECT create_distributed_table ('dist_table', 'd', colocate_with := 'none');

-- check that d is the distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

-- alter distribution column to e
SELECT alter_distributed_table('dist_table', distribution_column := 'e');

-- right now column cache is: d pos 1, e pos 2
-- check that e is the distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

ALTER TABLE dist_table ADD COLUMN a int DEFAULT 4;
ALTER TABLE dist_table ADD COLUMN b int DEFAULT 5;

-- right now column cache is: d pos 1, e pos 2, a pos 3, b pos 4

-- alter distribution column to a
SELECT alter_distributed_table('dist_table', distribution_column := 'a');

-- right now column cache hasn't changed
-- check that a is the distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

ALTER TABLE dist_table DROP COLUMN d;
ALTER TABLE dist_table DROP COLUMN e;
-- right now column cache is: d pos 1 attisdropped=true, e pos 2 attisdropped=true, a pos 3, b pos 4

-- alter distribution column to b
SELECT alter_distributed_table('dist_table', distribution_column := 'b');
-- column cache is: a pos 1, b pos 2 -> configuration with which we started these drop column tests
-- check that b is the distribution column
SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables
    WHERE table_name = 'dist_table'::regclass;

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

-- test altering partitioned table colocate_with:none
CREATE TABLE foo (x int, y int, t timestamptz default now()) PARTITION BY RANGE (t);
CREATE TABLE foo_1 PARTITION of foo for VALUES FROM ('2022-01-01') TO ('2022-12-31');
CREATE TABLE foo_2 PARTITION of foo for VALUES FROM ('2023-01-01') TO ('2023-12-31');

SELECT create_distributed_table('foo','x');

CREATE TABLE foo_bar (x int, y int, t timestamptz default now()) PARTITION BY RANGE (t);
CREATE TABLE foo_bar_1 PARTITION of foo_bar for VALUES FROM ('2022-01-01') TO ('2022-12-31');
CREATE TABLE foo_bar_2 PARTITION of foo_bar for VALUES FROM ('2023-01-01') TO ('2023-12-31');

SELECT create_distributed_table('foo_bar','x');

SELECT COUNT(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid::regclass::text in ('foo', 'foo_1', 'foo_2', 'foo_bar', 'foo_bar_1', 'foo_bar_2');

SELECT alter_distributed_table('foo', colocate_with => 'none');

SELECT COUNT(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid::regclass::text in ('foo', 'foo_1', 'foo_2', 'foo_bar', 'foo_bar_1', 'foo_bar_2');

-- test references
CREATE TABLE referenced_dist_table (a INT UNIQUE);
CREATE TABLE referenced_ref_table (a INT UNIQUE);
CREATE TABLE table_with_references (a1 INT UNIQUE, a2 INT);
CREATE TABLE referencing_dist_table (a INT);

SELECT create_distributed_table('referenced_dist_table', 'a', colocate_with:='none');
SELECT create_reference_table('referenced_ref_table');
SELECT create_distributed_table('table_with_references', 'a1', colocate_with:='referenced_dist_table');
SELECT create_distributed_table('referencing_dist_table', 'a', colocate_with:='referenced_dist_table');

ALTER TABLE table_with_references ADD FOREIGN KEY (a1) REFERENCES referenced_dist_table(a);
ALTER TABLE table_with_references ADD FOREIGN KEY (a2) REFERENCES referenced_ref_table(a);
ALTER TABLE referencing_dist_table ADD FOREIGN KEY (a) REFERENCES table_with_references(a1);

SET client_min_messages TO WARNING;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1,2;
SELECT alter_distributed_table('table_with_references', shard_count := 12, cascade_to_colocated := true);
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1,2;
SELECT alter_distributed_table('table_with_references', shard_count := 10, cascade_to_colocated := false);
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'table_with_references' OR confrelid::regclass::text = 'table_with_references') AND contype = 'f' ORDER BY 1,2;


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


-- test altering columnar table
CREATE TABLE columnar_table (a INT) USING columnar;
SELECT create_distributed_table('columnar_table', 'a', colocate_with:='none');
SELECT table_name::text, shard_count, access_method FROM public.citus_tables WHERE table_name::text = 'columnar_table';
SELECT alter_distributed_table('columnar_table', shard_count:=6);
SELECT table_name::text, shard_count, access_method FROM public.citus_tables WHERE table_name::text = 'columnar_table';


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


-- test changing shard count into a default colocation group with shard split
-- ensure there is no colocation group with 23 shards
SELECT count(*) FROM pg_dist_colocation WHERE shardcount = 23;
SET citus.shard_count TO 23;

CREATE TABLE shard_split_table (a int, b int);
SELECT create_distributed_table ('shard_split_table', 'a');
SELECT 1 FROM isolate_tenant_to_new_shard('shard_split_table', 5, shard_transfer_mode => 'block_writes');

-- show the difference in pg_dist_colocation and citus_tables shard counts
SELECT
	(
		SELECT shardcount FROM pg_dist_colocation WHERE colocationid IN
		(
			SELECT colocation_id FROM public.citus_tables WHERE table_name = 'shard_split_table'::regclass
		)
	) AS "pg_dist_colocation",
	(SELECT shard_count FROM public.citus_tables WHERE table_name = 'shard_split_table'::regclass) AS "citus_tables";

SET citus.shard_count TO 4;

-- distribute another table and then change shard count to 23
CREATE TABLE shard_split_table_2 (a int, b int);
SELECT create_distributed_table ('shard_split_table_2', 'a');
SELECT alter_distributed_table ('shard_split_table_2', shard_count:=23, cascade_to_colocated:=false);

SELECT a.colocation_id = b.colocation_id FROM public.citus_tables a, public.citus_tables b
	WHERE a.table_name = 'shard_split_table'::regclass AND b.table_name = 'shard_split_table_2'::regclass;

SELECT shard_count FROM public.citus_tables WHERE table_name = 'shard_split_table_2'::regclass;


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

-- test colocating with single shard table
CREATE TABLE single_shard_table (a INT);
SELECT create_distributed_table('single_shard_table', null);
SELECT alter_distributed_table('dist_table', colocate_with:='single_shard_table');

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

-- test long table names
SET client_min_messages TO DEBUG1;
CREATE TABLE abcde_0123456789012345678901234567890123456789012345678901234567890123456789 (x int, y int);
SELECT create_distributed_table('abcde_0123456789012345678901234567890123456789012345678901234567890123456789', 'x');
SELECT alter_distributed_table('abcde_0123456789012345678901234567890123456789012345678901234567890123456789', distribution_column := 'y');
RESET client_min_messages;

-- test long partitioned table names
CREATE TABLE partition_lengths
(
    tenant_id integer NOT NULL,
    timeperiod timestamp without time zone NOT NULL,
    inserted_utc timestamp without time zone NOT NULL DEFAULT now()
) PARTITION BY RANGE (timeperiod);

SELECT create_distributed_table('partition_lengths', 'tenant_id');
CREATE TABLE partition_lengths_p2020_09_28_12345678901234567890123456789012345678901234567890 PARTITION OF partition_lengths FOR VALUES FROM ('2020-09-28 00:00:00') TO ('2020-09-29 00:00:00');

-- verify alter_distributed_table works with long partition names
SELECT alter_distributed_table('partition_lengths', shard_count := 29, cascade_to_colocated := false);

-- test long partition table names
ALTER TABLE partition_lengths_p2020_09_28_12345678901234567890123456789012345678901234567890 RENAME TO partition_lengths_p2020_09_28;
ALTER TABLE partition_lengths RENAME TO partition_lengths_12345678901234567890123456789012345678901234567890;

-- verify alter_distributed_table works with long partitioned table names
SELECT alter_distributed_table('partition_lengths_12345678901234567890123456789012345678901234567890', shard_count := 17, cascade_to_colocated := false);

-- verify that alter_distributed_table doesn't change the access methods for the views on the table
CREATE TABLE test_am_matview(a int);
SELECT create_distributed_table('test_am_matview' ,'a', colocate_with:='none');
CREATE MATERIALIZED VIEW test_mat_view_am USING COLUMNAR AS SELECT count(*), a FROM test_am_matview GROUP BY a ;
SELECT alter_distributed_table('test_am_matview', shard_count:= 52);
SELECT amname FROM pg_am WHERE oid IN (SELECT relam FROM pg_class WHERE relname ='test_mat_view_am');

-- verify that alter_distributed_table works if it has dependent views and materialized views
-- set colocate_with explicitly to not to affect other tables
CREATE SCHEMA schema_to_test_alter_dist_table;
SET search_path to schema_to_test_alter_dist_table;

CREATE TABLE test_alt_dist_table_1(a int, b int);
SELECT create_distributed_table('test_alt_dist_table_1', 'a', colocate_with => 'None');

CREATE TABLE test_alt_dist_table_2(a int, b int);
SELECT create_distributed_table('test_alt_dist_table_2', 'a', colocate_with => 'test_alt_dist_table_1');

CREATE VIEW dependent_view_1 AS SELECT test_alt_dist_table_2.* FROM test_alt_dist_table_2;
CREATE VIEW dependent_view_2 AS SELECT test_alt_dist_table_2.* FROM test_alt_dist_table_2 JOIN test_alt_dist_table_1 USING(a);

CREATE MATERIALIZED VIEW dependent_mat_view_1 AS SELECT test_alt_dist_table_2.* FROM test_alt_dist_table_2;

-- Alter owner to make sure that alter_distributed_table doesn't change view's owner
SET client_min_messages TO WARNING;
CREATE USER alter_dist_table_test_user;
SELECT 1 FROM run_command_on_workers($$CREATE USER alter_dist_table_test_user$$);

ALTER VIEW dependent_view_1 OWNER TO alter_dist_table_test_user;
ALTER VIEW dependent_view_2 OWNER TO alter_dist_table_test_user;
ALTER MATERIALIZED VIEW dependent_mat_view_1 OWNER TO alter_dist_table_test_user;

SELECT alter_distributed_table('test_alt_dist_table_1', shard_count:=12, cascade_to_colocated:=true);
SELECT viewowner FROM pg_views WHERE viewname IN ('dependent_view_1', 'dependent_view_2');
SELECT matviewowner FROM pg_matviews WHERE matviewname = 'dependent_mat_view_1';

-- Check the existence of the view on the worker node as well
SELECT run_command_on_workers($$SELECT viewowner FROM pg_views WHERE viewname = 'dependent_view_1'$$);
SELECT run_command_on_workers($$SELECT viewowner FROM pg_views WHERE viewname = 'dependent_view_2'$$);
-- It is expected to not have mat view on worker node
SELECT run_command_on_workers($$SELECT count(*) FROM pg_matviews WHERE matviewname = 'dependent_mat_view_1';$$);
RESET search_path;

DROP SCHEMA alter_distributed_table CASCADE;
DROP SCHEMA schema_to_test_alter_dist_table CASCADE;
DROP USER alter_dist_table_test_user;
