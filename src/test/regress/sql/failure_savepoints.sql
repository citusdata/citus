-- We have two different output files for this failure test because the
-- failure behaviour of SAVEPOINT and RELEASE commands are different if
-- we use the executor. If we use it, these commands error out if any of
-- the placement commands fail. Otherwise, we might mark the placement
-- as invalid and continue with a WARNING.

SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1; -- one shard per worker
SET citus.next_shard_id TO 100950;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 150;

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

-- simply fail at SAVEPOINT
SELECT citus.mitmproxy('conn.onQuery(query="^SAVEPOINT").kill()');

BEGIN;
INSERT INTO artists VALUES (5, 'Asher Lev');
SAVEPOINT s1;
DELETE FROM artists WHERE id=4;
RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM artists WHERE id IN (4, 5);

-- fail at RELEASE
SELECT citus.mitmproxy('conn.onQuery(query="^RELEASE").kill()');

BEGIN;
UPDATE artists SET name='a';
SAVEPOINT s1;
DELETE FROM artists WHERE id=4;
RELEASE SAVEPOINT s1;
ROLLBACK;

SELECT * FROM artists WHERE id IN (4, 5);

-- fail at ROLLBACK
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');

BEGIN;
INSERT INTO artists VALUES (5, 'Asher Lev');
SAVEPOINT s1;
DELETE FROM artists WHERE id=4;
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT * FROM artists WHERE id IN (4, 5);

-- fail at second RELEASE
SELECT citus.mitmproxy('conn.onQuery(query="^RELEASE").after(1).kill()');
BEGIN;
SAVEPOINT s1;
DELETE FROM artists WHERE id=4;
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (5, 'Jacob Kahn');
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id IN (4, 5);

-- fail at second ROLLBACK
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").after(1).kill()');

BEGIN;
SAVEPOINT s1;
UPDATE artists SET name='A' WHERE id=4;
ROLLBACK TO SAVEPOINT s1;
SAVEPOINT s2;
DELETE FROM artists WHERE id=5;
ROLLBACK TO SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id IN (4, 5);

SELECT citus.mitmproxy('conn.onQuery(query="^RELEASE").after(1).kill()');

-- Release after rollback
BEGIN;
SAVEPOINT s1;
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (6, 'John J. Audubon');
INSERT INTO artists VALUES (7, 'Emily Carr');
ROLLBACK TO s2;
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM artists WHERE id=7;

SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');

-- Recover from errors
\set VERBOSITY terse
BEGIN;
SAVEPOINT s1;
SAVEPOINT s2;
INSERT INTO artists VALUES (6, 'John J. Audubon');
INSERT INTO artists VALUES (7, 'Emily Carr');
INSERT INTO artists VALUES (7, 'Emily Carr');
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT * FROM artists WHERE id=6;

-- replication factor > 1
CREATE TABLE researchers (
  id bigint NOT NULL,
  lab_id int NOT NULL,
  name text NOT NULL
);
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- single shard, on both workers
SELECT create_distributed_table('researchers', 'lab_id', 'hash');


-- simply fail at SAVEPOINT
SELECT citus.mitmproxy('conn.onQuery(query="^SAVEPOINT").kill()');

BEGIN;
INSERT INTO researchers VALUES (7, 4, 'Jan Plaza');
SAVEPOINT s1;
INSERT INTO researchers VALUES (8, 4, 'Alonzo Church');
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
COMMIT;

-- should see correct results from healthy placement and one bad placement
SELECT * FROM researchers WHERE lab_id = 4;

UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardstate = 3 AND shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'researchers'::regclass
) RETURNING placementid;
TRUNCATE researchers;

-- fail at rollback
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');

BEGIN;
INSERT INTO researchers VALUES (7, 4, 'Jan Plaza');
SAVEPOINT s1;
INSERT INTO researchers VALUES (8, 4, 'Alonzo Church');
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
COMMIT;

-- should see correct results from healthy placement and one bad placement
SELECT * FROM researchers WHERE lab_id = 4;

UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardstate = 3 AND shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'researchers'::regclass
) RETURNING placementid;
TRUNCATE researchers;

-- fail at release
SELECT citus.mitmproxy('conn.onQuery(query="^RELEASE").kill()');

BEGIN;
INSERT INTO researchers VALUES (7, 4, 'Jan Plaza');
SAVEPOINT s1;
INSERT INTO researchers VALUES (8, 4, 'Alonzo Church');
ROLLBACK TO s1;
RELEASE SAVEPOINT s1;
COMMIT;

-- should see correct results from healthy placement and one bad placement
SELECT * FROM researchers WHERE lab_id = 4;

UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardstate = 3 AND shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'researchers'::regclass
) RETURNING placementid;
TRUNCATE researchers;

-- test that we don't mark reference placements unhealthy
CREATE TABLE ref(a int, b int);
SELECT create_reference_table('ref');

SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
SAVEPOINT start;
INSERT INTO ref VALUES (1001,2);
SELECT * FROM ref;
ROLLBACK TO SAVEPOINT start;
SELECT * FROM ref;
END;

-- clean up
SELECT citus.mitmproxy('conn.allow()');
DROP TABLE artists;
DROP TABLE researchers;
DROP TABLE ref;
