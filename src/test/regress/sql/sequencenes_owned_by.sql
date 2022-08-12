CREATE SCHEMA "sequence tests";
SET search_path TO "sequence tests";

CREATE SEQUENCE "sc 1";

-- the same sequence is on nextval and owned by the same column
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'));
ALTER SEQUENCE "sc 1" OWNED BY test.b;
SELECT create_distributed_table('test','a');

-- show that "sc 1" is distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass$$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND  objid = '"sequence tests"."sc 1"'::regclass;

-- this is not supported for already distributed tables, which we might relax in the future
ALTER SEQUENCE "sc 1" OWNED BY test.b;

-- drop cascades into the sequence as well
DROP TABLE test CASCADE;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass$$);


CREATE SEQUENCE "sc 1";

-- a sequence is on nextval and owned by another column
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'), c bigint);
ALTER SEQUENCE "sc 1" OWNED BY test.c;
SELECT create_distributed_table('test','a');

-- show that "sc 1" is distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass$$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND  objid = '"sequence tests"."sc 1"'::regclass;

-- drop cascades into the schema as well
DROP TABLE test CASCADE;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass$$);

CREATE SEQUENCE "sc 1";
CREATE SEQUENCE "sc 2";

-- a different sequence is on nextval and owned by another column
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'), c bigint);
ALTER SEQUENCE "sc 2" OWNED BY test.c;
SELECT create_distributed_table('test','a');

-- show that "sc 1" and "sc 2" are distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text || ',' || '"sequence tests"."sc 2"'::regclass::text; $$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND (objid = '"sequence tests"."sc 1"'::regclass OR objid = '"sequence tests"."sc 2"'::regclass) ORDER BY 1;

-- drop cascades into the sc2 as well as it is OWNED BY
DROP TABLE test CASCADE;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 2"'::regclass::text; $$);

-- and, we manually drop sc1
DROP SEQUENCE "sc 1";
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text; $$);

CREATE SEQUENCE "sc 1";
CREATE SEQUENCE "sc 2";

-- a different sequence is on nextval, one different column owned by a sequence, and one bigserial
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'), c bigint, d bigserial);
ALTER SEQUENCE "sc 2" OWNED BY test.c;
SELECT create_distributed_table('test','a');

-- show that "sc 1", "sc 2" and test_d_seq are distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text || ',' || '"sequence tests"."sc 2"'::regclass::text || ',' || '"sequence tests"."test_d_seq"'::regclass::text; $$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND (objid = '"sequence tests"."sc 1"'::regclass OR objid = '"sequence tests"."sc 2"'::regclass OR objid = '"sequence tests"."test_d_seq"'::regclass) ORDER BY 1;

-- drop cascades into the schema as well
DROP TABLE test CASCADE;


CREATE SEQUENCE "sc 1";
CREATE SEQUENCE "sc 2";

-- a different sequence is on nextval, one  column owned by a sequence and it is bigserial
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'), c bigint, d bigserial);
ALTER SEQUENCE "sc 2" OWNED BY test.d;
SELECT create_distributed_table('test','a');

-- show that "sc 1" and "sc 2" are distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text || ',' || '"sequence tests"."sc 2"'::regclass::text || ',' || '"sequence tests"."test_d_seq"'::regclass::text; $$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND (objid = '"sequence tests"."sc 1"'::regclass OR objid = '"sequence tests"."sc 2"'::regclass OR objid = '"sequence tests"."test_d_seq"'::regclass) ORDER BY 1;

-- drop cascades into the sc2 and test_d_seq as well
DROP TABLE test CASCADE;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 2"'::regclass::text $$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."test_d_seq"'::regclass::text $$);

-- and, we manually drop sc1
DROP SEQUENCE "sc 1";
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text; $$);

CREATE SEQUENCE "sc 1";
CREATE SEQUENCE "sc 2";

-- a different sequence is on nextval, one  column owned by multiple sequences and it is bigserial
CREATE TABLE test(a int, b bigint default nextval ('"sc 1"'), c bigint, d bigserial);
ALTER SEQUENCE "sc 1" OWNED BY test.d;
ALTER SEQUENCE "sc 2" OWNED BY test.d;

SELECT create_distributed_table('test','a');

-- show that "sc 1", "sc 2" and test_d_seq are distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text || ',' || '"sequence tests"."sc 2"'::regclass::text || ',' || '"sequence tests"."test_d_seq"'::regclass::text; $$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND (objid = '"sequence tests"."sc 1"'::regclass OR objid = '"sequence tests"."sc 2"'::regclass OR objid = '"sequence tests"."test_d_seq"'::regclass) ORDER BY 1;

-- drop cascades into the all the sequences as well
DROP TABLE test CASCADE;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 2"'::regclass::text $$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."test_d_seq"'::regclass::text $$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text; $$);


-- Citus local tables handles sequences slightly differently, so lets have one complex example
-- which is combination of all the examples above
CREATE SEQUENCE "sc 1";
CREATE SEQUENCE "sc 2";
CREATE SEQUENCE "sc 3";

CREATE TABLE date_partitioned_citus_local_table_seq( measureid bigserial, col_a bigint, col_b bigserial, eventdate date, measure_data jsonb, PRIMARY KEY (measureid, eventdate)) PARTITION BY RANGE(eventdate);
SELECT create_time_partitions('date_partitioned_citus_local_table_seq', INTERVAL '1 month', '2022-01-01', '2021-01-01');

ALTER SEQUENCE "sc 1" OWNED BY date_partitioned_citus_local_table_seq.col_a;
ALTER SEQUENCE "sc 2" OWNED BY date_partitioned_citus_local_table_seq.col_a;
ALTER SEQUENCE "sc 3" OWNED BY date_partitioned_citus_local_table_seq.col_b;
ALTER SEQUENCE "sc 2" OWNED BY date_partitioned_citus_local_table_seq.col_b;

SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table_seq');


-- show that "sc 1", "sc 2" and test_d_seq are distributed
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass::text || ',' || '"sequence tests"."sc 2"'::regclass::text ||  ',' || '"sequence tests"."sc 3"'::regclass::text || ',' || '"sequence tests"."date_partitioned_citus_local_table_seq_col_b_seq"'::regclass::text; $$);
SELECT
	pg_identify_object_as_address(classid, objid, objsubid)
FROM pg_dist_object WHERE classid = 1259 AND (objid IN ('"sequence tests"."sc 1"'::regclass, '"sequence tests"."sc 2"'::regclass, '"sequence tests"."sc 3"'::regclass, '"sequence tests"."date_partitioned_citus_local_table_seq_col_b_seq"'::regclass)) ORDER BY 1;

-- this is not supported for Citus local tables as well, one day we might relax
ALTER SEQUENCE "sc 2" OWNED BY date_partitioned_citus_local_table_seq.col_a;

-- drop cascades to all sequneces
DROP TABLE date_partitioned_citus_local_table_seq;
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 1"'::regclass$$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 2"'::regclass$$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."sc 3"'::regclass$$);
SELECT DISTINCT result FROM run_command_on_workers($$SELECT '"sequence tests"."date_partitioned_citus_local_table_seq_col_b_seq"'::regclass$$);

DROP SCHEMA "sequence tests" CASCADE;
