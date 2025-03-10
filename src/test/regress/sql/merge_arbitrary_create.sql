DROP SCHEMA IF EXISTS merge_arbitrary_schema CASCADE;
CREATE SCHEMA merge_arbitrary_schema;
SET search_path TO merge_arbitrary_schema;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 6000000;
CREATE TABLE target_cj(tid int, src text, val int);
CREATE TABLE source_cj1(sid1 int, src1 text, val1 int);
CREATE TABLE source_cj2(sid2 int, src2 text, val2 int);

SELECT create_distributed_table('target_cj', 'tid');
SELECT create_distributed_table('source_cj1', 'sid1');
SELECT create_distributed_table('source_cj2', 'sid2');

CREATE TABLE prept(t1 int, t2 int);
CREATE TABLE preps(s1 int, s2 int);

SELECT create_distributed_table('prept', 't1'), create_distributed_table('preps', 's1');

PREPARE insert(int, int, int) AS
MERGE INTO prept
USING (SELECT $2, s1, s2 FROM preps WHERE s2 > $3) as foo
ON prept.t1 = foo.s1
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + $1
WHEN NOT MATCHED THEN
        INSERT VALUES(s1, s2);

PREPARE delete(int) AS
MERGE INTO prept
USING preps
ON prept.t1 = preps.s1
WHEN MATCHED AND prept.t2 = $1 THEN
        DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 1;

-- Citus local tables
CREATE TABLE t1(id int, val int);
CREATE TABLE s1(id int, val int);

SELECT citus_add_local_table_to_metadata('t1');
SELECT citus_add_local_table_to_metadata('s1');

-- Test prepared statements with repartition
CREATE TABLE pg_target(id int, val int);
CREATE TABLE pg_source(id int, val int, const int);
CREATE TABLE citus_target(id int, val int);
CREATE TABLE citus_source(id int, val int, const int);
SELECT citus_add_local_table_to_metadata('pg_target');
SELECT citus_add_local_table_to_metadata('pg_source');

--
-- Load same set of data to both Postgres and Citus tables
--
CREATE OR REPLACE FUNCTION setup_data() RETURNS VOID AS $$
    INSERT INTO pg_source SELECT i, i+1, 1 FROM generate_series(1, 10000) i;
    INSERT INTO pg_target SELECT i, 1 FROM generate_series(5001, 10000) i;
    INSERT INTO citus_source SELECT i, i+1, 1 FROM generate_series(1, 10000) i;
    INSERT INTO citus_target SELECT i, 1 FROM generate_series(5001, 10000) i;
$$
LANGUAGE SQL;

--
-- Compares the final target tables, merge-modified data, of both Postgres and Citus tables
--
CREATE OR REPLACE FUNCTION check_data(table1_name text, column1_name text, table2_name text, column2_name text)
RETURNS VOID AS $$
DECLARE
    table1_avg numeric;
    table2_avg numeric;
BEGIN
    EXECUTE format('SELECT COALESCE(AVG(%I), 0) FROM %I', column1_name, table1_name) INTO table1_avg;
    EXECUTE format('SELECT COALESCE(AVG(%I), 0) FROM %I', column2_name, table2_name) INTO table2_avg;

    IF table1_avg > table2_avg THEN
        RAISE EXCEPTION 'The average of %.% is greater than %.%', table1_name, column1_name, table2_name, column2_name;
    ELSIF table1_avg < table2_avg THEN
        RAISE EXCEPTION 'The average of %.% is less than %.%', table1_name, column1_name, table2_name, column2_name;
    ELSE
        RAISE NOTICE 'The average of %.% is equal to %.%', table1_name, column1_name, table2_name, column2_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION compare_data() RETURNS VOID AS $$
    SELECT check_data('pg_target', 'id', 'citus_target', 'id');
    SELECT check_data('pg_target', 'val', 'citus_target', 'val');
$$
LANGUAGE SQL;

--
-- Target and source are distributed, and non-colocated
--
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');
