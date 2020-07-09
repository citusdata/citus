CREATE SCHEMA multi_subtransactions;
SET search_path TO 'multi_subtransactions';

CREATE TABLE artists (
    id bigint NOT NULL,
    name text NOT NULL
);
SELECT create_distributed_table('artists', 'id');

-- add some data
INSERT INTO artists VALUES (1, 'Pablo Picasso');
INSERT INTO artists VALUES (2, 'Vincent van Gogh');
INSERT INTO artists VALUES (3, 'Claude Monet');
INSERT INTO artists VALUES (4, 'William Kurelek');

-- RELEASE SAVEPOINT
BEGIN;
INSERT INTO artists VALUES (5, 'Asher Lev');
SAVEPOINT s1;
DELETE FROM artists WHERE id=5;
RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM artists WHERE id=5;

-- ROLLBACK TO SAVEPOINT
BEGIN;
INSERT INTO artists VALUES (5, 'Asher Lev');
SAVEPOINT s1;
DELETE FROM artists WHERE id=5;
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT * FROM artists WHERE id=5;

-- Serial sub-transaction releases
BEGIN;
SAVEPOINT s1;
DELETE FROM artists WHERE id=5;
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (5, 'Jacob Kahn');
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id=5;

-- Serial sub-transaction rollbacks
BEGIN;
SAVEPOINT s1;
UPDATE artists SET name='A' WHERE id=5;
ROLLBACK TO SAVEPOINT s1;
SAVEPOINT s2;
DELETE FROM artists WHERE id=5;
ROLLBACK TO SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id=5;

-- Multiple sub-transaction activity before first query
BEGIN;
SAVEPOINT s0;
SAVEPOINT s1;
SAVEPOINT s2;
SAVEPOINT s3;
ROLLBACK TO SAVEPOINT s2;
RELEASE SAVEPOINT s1;
INSERT INTO artists VALUES (6, 'John J. Audubon');
ROLLBACK TO SAVEPOINT s0;
INSERT INTO artists VALUES (6, 'Emily Carr');
COMMIT;

SELECT * FROM artists WHERE id=6;

-- Release after rollback
BEGIN;
SAVEPOINT s1;
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (7, 'John J. Audubon');
ROLLBACK TO s2;
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id=7;

-- Recover from errors
\set VERBOSITY terse
BEGIN;
SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (7, NULL);
ROLLBACK TO SAVEPOINT s1;
COMMIT;

-- Don't recover from errors
BEGIN;
SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (7, NULL);
SAVEPOINT s3;
ROLLBACK TO SAVEPOINT s3;
COMMIT;

-- Recover from multi-shard modify errors
BEGIN;
INSERT INTO artists VALUES (8, 'Sogand');
SAVEPOINT s1;
UPDATE artists SET name = NULL;
ROLLBACK TO s1;
INSERT INTO artists VALUES (9, 'Mohsen Namjoo');
COMMIT;

SELECT * FROM artists WHERE id IN (7, 8, 9) ORDER BY id;

-- Recover from multi-shard copy shutdown failure.
-- Constraint check for non-partition columns happen only at copy shutdown.
BEGIN;
DELETE FROM artists;
SAVEPOINT s1;
INSERT INTO artists SELECT i, NULL FROM generate_series(1, 5) i;
ROLLBACK TO s1;
INSERT INTO artists VALUES (10, 'Mahmoud Farshchian');
COMMIT;

SELECT * FROM artists WHERE id IN (9, 10) ORDER BY id;

-- Recover from multi-shard copy send failure.
-- Constraint check for partition column happens at copy send.
BEGIN;
DELETE FROM artists;
SAVEPOINT s1;
INSERT INTO artists SELECT NULL, NULL FROM generate_series(1, 5) i;
ROLLBACK TO s1;
INSERT INTO artists VALUES (11, 'Egon Schiele');
COMMIT;

SELECT * FROM artists WHERE id IN (10, 11) ORDER BY id;

-- Recover from multi-shard copy startup failure.
-- Check for existence of a value for partition columnn happens at copy startup.
BEGIN;
DELETE FROM artists;
SAVEPOINT s1;
INSERT INTO artists(name) SELECT 'a' FROM generate_series(1, 5) i;
ROLLBACK TO s1;
INSERT INTO artists VALUES (12, 'Marc Chagall');
COMMIT;

SELECT * FROM artists WHERE id IN (11, 12) ORDER BY id;

-- Recover from multi-shard CTE modify failures
create table t1(a int, b int);
create table t2(a int, b int CHECK(b > 0));

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1190000;

select create_distributed_table('t1', 'a'),
       create_distributed_table('t2', 'a');

begin;
insert into t2 select i, i+1 from generate_series(1, 3) i;
with r AS (
    update t2 set b = b + 1
    returning *
) insert into t1 select * from r;
savepoint s1;
with r AS (
    update t1 set b = b - 10
    returning *
) insert into t2 select * from r;
rollback to savepoint s1;
savepoint s2;
with r AS (
    update t2 set b = b - 10
    returning *
) insert into t1 select * from r;
rollback to savepoint s2;
savepoint s3;
with r AS (
    insert into t2 select i, i+1 from generate_series(-10,-5) i
    returning *
) insert into t1 select * from r;
rollback to savepoint s3;
savepoint s4;
with r AS (
    insert into t1 select i, i+1 from generate_series(-10,-5) i
    returning *
) insert into t2 select * from r;
rollback to savepoint s4;
with r AS (
    update t2 set b = b + 1
    returning *
) insert into t1 select * from r;
commit;

select * from t2 order by a, b;
select * from t1 order by a, b;

drop table t1, t2;

-- ===================================================================
-- Tests for replication factor > 1
-- ===================================================================

CREATE TABLE researchers (
  id bigint NOT NULL,
  lab_id int NOT NULL,
  name text NOT NULL
);
SET citus.shard_count TO 2;
SELECT create_distributed_table('researchers', 'lab_id', 'hash');

-- Basic rollback and release
BEGIN;
INSERT INTO researchers VALUES (7, 4, 'Jan Plaza');
SAVEPOINT s1;
INSERT INTO researchers VALUES (8, 4, 'Alonzo Church');
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM researchers WHERE id in (7, 8);

-- Recover from failure on one of nodes
BEGIN;
SAVEPOINT s1;
INSERT INTO researchers VALUES (11, 11, 'Dana Scott');
INSERT INTO researchers VALUES (NULL, 10, 'Stephen Kleene');
ROLLBACK TO SAVEPOINT s1;
INSERT INTO researchers VALUES (12, 10, 'Stephen Kleene');
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

-- Don't recover, but rollback
BEGIN;
SAVEPOINT s1;
INSERT INTO researchers VALUES (NULL, 10, 'Raymond Smullyan');
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
ROLLBACK;

SELECT * FROM researchers WHERE lab_id=10;

-- Don't recover, and commit
BEGIN;
SAVEPOINT s1;
INSERT INTO researchers VALUES (NULL, 10, 'Raymond Smullyan');
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

-- Implicit savepoints via pl/pgsql exceptions
BEGIN;
DO $$
BEGIN
  INSERT INTO researchers VALUES (15, 10, 'Melvin Fitting');
  INSERT INTO researchers VALUES (NULL, 10, 'Raymond Smullyan');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

BEGIN;
DO $$
BEGIN
  INSERT INTO researchers VALUES (15, 10, 'Melvin Fitting');
  RAISE EXCEPTION plpgsql_error;
EXCEPTION
    WHEN plpgsql_error THEN
        RAISE NOTICE 'caught manual plpgsql_error';
END $$;
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

BEGIN;
DO $$
BEGIN
  INSERT INTO researchers VALUES (15, 10, 'Melvin Fitting');
  INSERT INTO researchers VALUES (NULL, 10, 'Raymond Smullyan');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE EXCEPTION not_null_violation; -- rethrow it
END $$;
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

-- Insert something after catching error.
BEGIN;
DO $$
BEGIN
  INSERT INTO researchers VALUES (15, 10, 'Melvin Fitting');
  INSERT INTO researchers VALUES (NULL, 10, 'Raymond Smullyan');
EXCEPTION
    WHEN not_null_violation THEN
        INSERT INTO researchers VALUES (32, 10, 'Raymond Smullyan');
END $$;
COMMIT;

SELECT * FROM researchers WHERE lab_id=10;

-- Verify that we don't have a memory leak in subtransactions
-- See https://github.com/citusdata/citus/pull/4000

CREATE FUNCTION text2number(v_value text) RETURNS numeric
    LANGUAGE plpgsql VOLATILE
    AS $$
BEGIN
 RETURN v_value::numeric;
exception
    when others then
        return null;
END;
$$;

-- if we leak at least an integer in each subxact, then size of TopTransactionSize
-- will be way beyond the 50k limit. If issue #3999 happens, then this will also take
-- a long time, since for each row we will create a memory context that is not destroyed
-- until the end of command.
SELECT max(text2number('1234')), max(public.top_transaction_context_size()) > 50000 AS leaked
FROM generate_series(1, 20000);

-- Clean-up
SET client_min_messages TO ERROR;
DROP SCHEMA multi_subtransactions CASCADE;
