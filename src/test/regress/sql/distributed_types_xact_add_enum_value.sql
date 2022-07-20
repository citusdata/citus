SET citus.next_shard_id TO 20040000;

CREATE SCHEMA xact_enum_type;
SET search_path TO xact_enum_type;
SET citus.shard_count TO 4;

-- transaction block with simple type
BEGIN;
CREATE TYPE xact_enum_edit AS ENUM ('yes', 'no');
CREATE TABLE t1 (a int PRIMARY KEY, b xact_enum_edit);
SELECT create_distributed_table('t1','a');
INSERT INTO t1 VALUES (1, 'yes');
SELECT * FROM t1;
COMMIT;

BEGIN;
ALTER TYPE xact_enum_edit ADD VALUE 'maybe';
ABORT;
-- maybe should not be on the workers
SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder ASC) FROM pg_enum WHERE enumtypid = 'xact_enum_type.xact_enum_edit'::regtype;
SELECT run_command_on_workers($$SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder ASC) FROM pg_enum WHERE enumtypid = 'xact_enum_type.xact_enum_edit'::regtype;$$);

BEGIN;
ALTER TYPE xact_enum_edit ADD VALUE 'maybe';
COMMIT;
-- maybe should be on the workers (pg12 and above)
SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder ASC) FROM pg_enum WHERE enumtypid = 'xact_enum_type.xact_enum_edit'::regtype;
SELECT run_command_on_workers($$SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder ASC) FROM pg_enum WHERE enumtypid = 'xact_enum_type.xact_enum_edit'::regtype;$$);

-- clear objects
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA xact_enum_type CASCADE;
