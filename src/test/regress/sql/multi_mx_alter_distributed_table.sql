CREATE SCHEMA mx_alter_distributed_table;
SET search_path TO mx_alter_distributed_table;
SET citus.shard_replication_factor TO 1;

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

SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%';
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%' GROUP BY "Colocation ID" ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SET client_min_messages TO WARNING;
SELECT alter_distributed_table('adt_table', shard_count:=6, cascade_to_colocated:=true);
SET client_min_messages TO DEFAULT;

SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%';
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%' GROUP BY "Colocation ID" ORDER BY 1;
SELECT conrelid::regclass::text AS "Referencing Table", pg_get_constraintdef(oid, true) AS "Definition" FROM  pg_constraint
    WHERE (conrelid::regclass::text = 'adt_col' OR confrelid::regclass::text = 'adt_col') ORDER BY 1;

SELECT alter_distributed_table('adt_table', distribution_column:='b', colocate_with:='none');

SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%';
SELECT STRING_AGG("Name"::text, ', ' ORDER BY 1) AS "Colocation Groups" FROM public.citus_tables WHERE "Name"::text LIKE 'adt%' GROUP BY "Colocation ID" ORDER BY 1;
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

SELECT "Name", "Citus Table Type", "Distribution Column", "Shard Count" FROM public.citus_tables WHERE "Name"::text = 'adt_table';

SET client_min_messages TO WARNING;
DROP SCHEMA mx_alter_distributed_table CASCADE;
