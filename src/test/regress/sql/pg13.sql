
create schema test_pg13;
set search_path to test_pg13;

SET citus.shard_replication_factor to 1;
SET citus.shard_count to 2;
SET citus.next_shard_id TO 65000;

CREATE TABLE dist_table (name char, age int);
CREATE INDEX name_index on dist_table(name);

SELECT create_distributed_table('dist_table', 'name');

SET client_min_messages to DEBUG1;
SET citus.log_remote_commands to ON;
-- make sure vacuum parallel doesn't error out
VACUUM (PARALLEL 2) dist_table;
VACUUM (PARALLEL 0) dist_table;
-- This should error out since -5 is not valid.
VACUUM (PARALLEL -5) dist_table;
-- This should error out since no number is given
VACUUM (PARALLEL) dist_table;

RESET client_min_messages;

-- test alter table alter column drop expression
CREATE TABLE generated_col_table(a int, b int GENERATED ALWAYS AS (a * 10) STORED);
SELECT create_distributed_table('generated_col_table', 'a');
INSERT INTO generated_col_table VALUES (1);
-- Make sure that we currently error out
ALTER TABLE generated_col_table ALTER COLUMN b DROP EXPRESSION;

RESET citus.log_remote_commands;

drop schema test_pg13 cascade;