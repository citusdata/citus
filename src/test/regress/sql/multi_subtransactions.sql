-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_nine;


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

-- ===================================================================
-- Tests for replication factor > 1
-- ===================================================================

CREATE TABLE researchers (
  id bigint NOT NULL,
  lab_id int NOT NULL,
  name text NOT NULL
);

SELECT master_create_distributed_table('researchers', 'lab_id', 'hash');
SELECT master_create_worker_shards('researchers', 2, 2);

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

-- Clean-up
DROP TABLE artists;
DROP TABLE researchers;
