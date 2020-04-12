SET citus.next_shard_id TO 20020000;

CREATE SCHEMA type_conflict;
SELECT run_command_on_workers($$CREATE SCHEMA type_conflict;$$);

-- create a type on a worker that should not cause data loss once overwritten with a type
-- from the coordinator
\c - - :public_worker_1_host :worker_1_port
SET citus.enable_ddl_propagation TO off;
SET search_path TO type_conflict;
CREATE TYPE my_precious_type AS (secret text, should bool);
CREATE TABLE local_table (a int, b my_precious_type);
INSERT INTO local_table VALUES (42, ('always bring a towel', true)::my_precious_type);
\c - - :master_host :master_port
SET search_path TO type_conflict;

-- overwrite the type on the worker from the coordinator. The type should be over written
-- but the data should not have been destroyed
CREATE TYPE my_precious_type AS (scatterd_secret text);

-- verify the data is retained
\c - - :public_worker_1_host :worker_1_port
SET search_path TO type_conflict;
-- show fields for table
  SELECT pg_class.relname,
         attname,
         atttype.typname
    FROM pg_attribute
    JOIN pg_class ON (attrelid = pg_class.oid)
    JOIN pg_type AS atttype ON (atttypid = atttype.oid)
   WHERE pg_class.relname = 'local_table'
     AND attnum > 0
ORDER BY attnum;

SELECT * FROM local_table;

\c - - :master_host :master_port
SET search_path TO type_conflict;

-- make sure worker_create_or_replace correctly generates new names while types are existing
SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type AS (a int, b int);');
SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type AS (a int, b int, c int);');
SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type AS (a int, b int, c int, d int);');

SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type_with_a_really_long_name_that_truncates AS (a int, b int);');
SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type_with_a_really_long_name_that_truncates AS (a int, b int, c int);');
SELECT worker_create_or_replace_object('CREATE TYPE type_conflict.multi_conflicting_type_with_a_really_long_name_that_truncates AS (a int, b int, c int, d int);');

-- verify they have been created with their names and attributes
SELECT pg_type.typname,
       string_agg(attname || ' ' || atttype.typname, ', ' ORDER BY attnum) AS fields
FROM pg_attribute
         JOIN pg_class ON (attrelid = pg_class.oid)
         JOIN pg_type ON (pg_class.reltype = pg_type.oid)
         JOIN pg_type AS atttype ON (atttypid = atttype.oid)
WHERE pg_type.typname LIKE 'multi_conflicting_type%'
GROUP BY pg_type.typname;

-- hide cascades
SET client_min_messages TO error;
DROP SCHEMA type_conflict CASCADE;
