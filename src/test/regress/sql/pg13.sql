
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
RESET citus.log_remote_commands;

-- test alter table alter column drop expression
CREATE TABLE generated_col_table(a int, b int GENERATED ALWAYS AS (a * 10) STORED);
SELECT create_distributed_table('generated_col_table', 'a');
INSERT INTO generated_col_table VALUES (1);
-- Make sure that we currently error out
ALTER TABLE generated_col_table ALTER COLUMN b DROP EXPRESSION;

-- alter view rename column works fine
CREATE VIEW v AS SELECT * FROM dist_table;
ALTER VIEW v RENAME age to new_age;
SELECT * FROM v;

-- row suffix notation works fine
CREATE TABLE ab (a int, b int);
SELECT create_distributed_table('ab','a');
INSERT INTO ab SELECT i, 2 * i FROM generate_series(1,20)i;
SELECT * FROM ab WHERE (ROW(a,b)).f1 > (ROW(10,30)).f1 ORDER BY 1,2;
SELECT * FROM ab WHERE (ROW(a,b)).f2 > (ROW(0,38)).f2 ORDER BY 1,2;

-- test normalized
CREATE TABLE text_table (name text);
SELECT create_distributed_table('text_table', 'name');
INSERT INTO text_table VALUES ('abc');
-- not normalized
INSERT INTO text_table VALUES (U&'\0061\0308bc');
SELECT name IS NORMALIZED FROM text_table ORDER BY 1;
SELECT is_normalized(name) FROM text_table ORDER BY 1;
SELECT normalize(name) FROM text_table ORDER BY 1;
INSERT INTO text_table VALUES (normalize(U&'\0061\0308bc', NFC));

-- test unicode escape
-- insert the word 'data' with unicode escapes
INSERT INTO text_table VALUES(U&'d\0061t\+000061');
-- insert the word слон
INSERT INTO text_table VALUES(U&'\0441\043B\043E\043D');
SELECT * FROM text_table ORDER BY 1;

drop schema test_pg13 cascade;