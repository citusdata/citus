CREATE SCHEMA mx_alter_distributed_table;
SET search_path TO mx_alter_distributed_table;
SET citus.shard_replication_factor TO 1;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1410000;

-- test alter_distributed_table UDF
CREATE TABLE adt_table (a INT, b INT);
CREATE TABLE adt_col (a INT UNIQUE, b INT);
CREATE TABLE adt_ref (a INT REFERENCES adt_col(a));

SELECT create_distributed_table('adt_table', 'a', colocate_with:='none');
SELECT create_distributed_table('adt_col', 'a', colocate_with:='adt_table');
SELECT create_distributed_table('adt_ref', 'a', colocate_with:='adt_table');

INSERT INTO adt_table VALUES (1, 2), (3, 4), (5, 6);
INSERT INTO adt_col VALUES (3, 4), (5, 6), (7, 8);
INSERT INTO adt_ref VALUES (3), (5);

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SET client_min_messages TO WARNING;
SELECT alter_distributed_table('adt_table', shard_count:=6, cascade_to_colocated:=true);
SET client_min_messages TO DEFAULT;

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SELECT alter_distributed_table('adt_table', distribution_column:='b', colocate_with:='none');

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text LIKE 'adt%';
SELECT STRING_AGG(table_name::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE table_name::text LIKE 'adt%' GROUP BY colocation_id ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SELECT * FROM adt_table ORDER BY 1;
SELECT * FROM adt_col ORDER BY 1;
SELECT * FROM adt_ref ORDER BY 1;

BEGIN;
INSERT INTO adt_table SELECT x, x+1 FROM generate_series(1, 1000) x;
SELECT alter_distributed_table('adt_table', distribution_column:='a');
SELECT COUNT(*) FROM adt_table;
END;

SELECT table_name, citus_table_type, distribution_column, shard_count FROM public.citus_tables WHERE table_name::text = 'adt_table';

-- test procedure colocation is preserved with alter_distributed_table
CREATE TABLE test_proc_colocation_0 (a float8);
SELECT create_distributed_table('test_proc_colocation_0', 'a');

CREATE OR REPLACE procedure proc_0(dist_key float8)
LANGUAGE plpgsql
AS $$
DECLARE
    res INT := 0;
BEGIN
    INSERT INTO mx_alter_distributed_table.test_proc_colocation_0 VALUES (dist_key);
    SELECT count(*) INTO res FROM mx_alter_distributed_table.test_proc_colocation_0;
    RAISE NOTICE 'Res: %', res;
    COMMIT;
END;$$;
SELECT create_distributed_function('proc_0(float8)', 'dist_key', 'test_proc_colocation_0' );

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0');

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
RESET client_min_messages;

-- shardCount is not null && list_length(colocatedTableList) = 1
SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 8);

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
RESET client_min_messages;

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0');

-- colocatewith is not null && list_length(colocatedTableList) = 1
SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 4);
CREATE TABLE test_proc_colocation_1 (a float8);
SELECT create_distributed_table('test_proc_colocation_1', 'a', colocate_with := 'none');
SELECT alter_distributed_table('test_proc_colocation_0', colocate_with := 'test_proc_colocation_1');

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
RESET client_min_messages;

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0');

-- shardCount is not null && cascade_to_colocated is true
SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 8, cascade_to_colocated := true);

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
RESET client_min_messages;

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0');

-- colocatewith is not null && cascade_to_colocated is true
SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 4, cascade_to_colocated := true);
CREATE TABLE test_proc_colocation_2 (a float8);
SELECT create_distributed_table('test_proc_colocation_2', 'a', colocate_with := 'none');
SELECT alter_distributed_table('test_proc_colocation_0', colocate_with := 'test_proc_colocation_2', cascade_to_colocated := true);

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
RESET client_min_messages;

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0');

-- try a case with more than one procedure
CREATE OR REPLACE procedure proc_1(dist_key float8)
LANGUAGE plpgsql
AS $$
DECLARE
    res INT := 0;
BEGIN
    INSERT INTO mx_alter_distributed_table.test_proc_colocation_0 VALUES (dist_key);
    SELECT count(*) INTO res FROM mx_alter_distributed_table.test_proc_colocation_0;
    RAISE NOTICE 'Res: %', res;
    COMMIT;
END;$$;
SELECT create_distributed_function('proc_1(float8)', 'dist_key', 'test_proc_colocation_0' );

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0', 'proc_1') ORDER BY proname;

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
CALL proc_1(2.0);
RESET client_min_messages;

SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 8, cascade_to_colocated := true);

SET client_min_messages TO DEBUG1;
CALL proc_0(1.0);
CALL proc_1(2.0);
RESET client_min_messages;

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0', 'proc_1') ORDER BY proname;

-- case which shouldn't preserve colocation for now
-- shardCount is not null && cascade_to_colocated is false
SELECT alter_distributed_table('test_proc_colocation_0', shard_count:= 18, cascade_to_colocated := false);

SELECT logicalrelid, colocationid FROM pg_dist_partition WHERE logicalrelid::regclass::text IN ('test_proc_colocation_0');
SELECT proname, colocationid FROM pg_proc JOIN pg_catalog.pg_dist_object ON pg_proc.oid = pg_catalog.pg_dist_object.objid WHERE proname IN ('proc_0', 'proc_1') ORDER BY proname;

SET client_min_messages TO WARNING;
DROP SCHEMA mx_alter_distributed_table CASCADE;
