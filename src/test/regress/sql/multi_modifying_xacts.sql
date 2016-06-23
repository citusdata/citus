
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1200000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1200000;


-- ===================================================================
-- test end-to-end modification functionality
-- ===================================================================

CREATE TABLE researchers (
	id bigint NOT NULL,
    lab_id int NOT NULL,
	name text NOT NULL
);

CREATE TABLE labs (
	id bigint NOT NULL,
	name text NOT NULL
);

SELECT master_create_distributed_table('researchers', 'lab_id', 'hash');
SELECT master_create_worker_shards('researchers', 2, 2);

SELECT master_create_distributed_table('labs', 'id', 'hash');
SELECT master_create_worker_shards('labs', 1, 1);

-- add some data
INSERT INTO researchers VALUES (1, 1, 'Donald Knuth');
INSERT INTO researchers VALUES (2, 1, 'Niklaus Wirth');
INSERT INTO researchers VALUES (3, 2, 'Tony Hoare');
INSERT INTO researchers VALUES (4, 2, 'Kenneth Iverson');

-- replace a researcher, reusing their id
BEGIN;
DELETE FROM researchers WHERE lab_id = 1 AND id = 2;
INSERT INTO researchers VALUES (2, 1, 'John Backus');
COMMIT;

SELECT name FROM researchers WHERE lab_id = 1 AND id = 2;

-- abort a modification
BEGIN;
DELETE FROM researchers WHERE lab_id = 1 AND id = 1;
ABORT;

SELECT name FROM researchers WHERE lab_id = 1 AND id = 1;

-- creating savepoints should work...
BEGIN;
INSERT INTO researchers VALUES (5, 3, 'Dennis Ritchie');
SAVEPOINT hire_thompson;
INSERT INTO researchers VALUES (6, 3, 'Ken Thompson');
COMMIT;

SELECT name FROM researchers WHERE lab_id = 3 AND id = 6;

-- even if created by PL/pgSQL...
BEGIN;
DO $$
BEGIN
	INSERT INTO researchers VALUES (10, 10, 'Edsger Dijkstra');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;

-- but rollback should not
BEGIN;
INSERT INTO researchers VALUES (7, 4, 'Jim Gray');
SAVEPOINT hire_engelbart;
INSERT INTO researchers VALUES (8, 4, 'Douglas Engelbart');
ROLLBACK TO hire_engelbart;
COMMIT;

SELECT name FROM researchers WHERE lab_id = 4;

BEGIN;
DO $$
BEGIN
	INSERT INTO researchers VALUES (NULL, 10, 'Edsger Dijkstra');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;


-- should be valid to edit labs after researchers...
BEGIN;
INSERT INTO researchers VALUES (8, 5, 'Douglas Engelbart');
INSERT INTO labs VALUES (5, 'Los Alamos');
COMMIT;

SELECT * FROM researchers, labs WHERE labs.id = researchers.lab_id;

-- but not the other way around (would require expanding xact participants)...
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
INSERT INTO researchers VALUES (9, 6, 'Leslie Lamport');
COMMIT;

-- this logic even applies to router SELECTs occurring after a modification:
-- selecting from the modified node is fine...
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
SELECT count(*) FROM researchers WHERE lab_id = 6;
ABORT;

-- but if a SELECT needs to go to new node, that's a problem...

BEGIN;

UPDATE pg_dist_shard_placement AS sp SET shardstate = 3
FROM   pg_dist_shard AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_1_port
AND    s.logicalrelid = 'researchers'::regclass;

INSERT INTO labs VALUES (6, 'Bell Labs');
SELECT count(*) FROM researchers WHERE lab_id = 6;
ABORT;

-- applies to DDL or COPY, too
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
ALTER TABLE labs ADD COLUMN text motto;
COMMIT;

BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
\copy labs from stdin delimiter ','
10,Weyland-Yutani
\.
COMMIT;

-- though the copy will work if before any modifications
BEGIN;
\copy labs from stdin delimiter ','
10,Weyland-Yutani
\.
SELECT name FROM labs WHERE id = 10;
INSERT INTO labs VALUES (6, 'Bell Labs');
COMMIT;

-- now, for some special failures...
CREATE TABLE objects (
	id bigint PRIMARY KEY,
	name text NOT NULL
);

SELECT master_create_distributed_table('objects', 'id', 'hash');
SELECT master_create_worker_shards('objects', 1, 2);

-- test primary key violations
BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO objects VALUES (1, 'orange');
COMMIT;

-- data shouldn't have persisted...
SELECT * FROM objects WHERE id = 1;

-- and placements should still be healthy...
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.shardstate = 1
AND    s.logicalrelid = 'objects'::regclass;

-- create trigger on one worker to reject certain values
\c - - - :worker_2_port

CREATE FUNCTION reject_bad() RETURNS trigger AS $rb$
    BEGIN
        IF (NEW.name = 'BAD') THEN
            RAISE 'illegal value';
        END IF;

        RETURN NEW;
    END;
$rb$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER reject_bad
AFTER INSERT ON objects_1200003
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad();

\c - - - :master_port

-- test partial failure; worker_1 succeeds, 2 fails
BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO objects VALUES (2, 'BAD');
INSERT INTO labs VALUES (7, 'E Corp');
COMMIT;

-- data should be persisted
SELECT * FROM objects WHERE id = 2;
SELECT * FROM labs WHERE id = 7;

-- but one placement should be bad
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_2_port
AND    sp.shardstate = 3
AND    s.logicalrelid = 'objects'::regclass;

DELETE FROM objects;

-- mark shards as healthy again; delete all data
UPDATE pg_dist_shard_placement AS sp SET shardstate = 1
FROM   pg_dist_shard AS s
WHERE  sp.shardid = s.shardid
AND    s.logicalrelid = 'objects'::regclass;

-- what if there are errors on different shards at different times?
\c - - - :worker_1_port
CREATE FUNCTION reject_bad() RETURNS trigger AS $rb$
    BEGIN
        IF (NEW.name = 'BAD') THEN
            RAISE 'illegal value';
        END IF;

        RETURN NEW;
    END;
$rb$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER reject_bad
AFTER INSERT ON labs_1200002
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad();

\c - - - :master_port

BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO objects VALUES (2, 'BAD');
INSERT INTO labs VALUES (8, 'Aperture Science');
INSERT INTO labs VALUES (9, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects WHERE id = 1;
SELECT * FROM labs WHERE id = 8;

-- all placements should remain healthy
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.shardstate = 1
AND    (s.logicalrelid = 'objects'::regclass OR
	    s.logicalrelid = 'labs'::regclass);

-- what if the failures happen at COMMIT time?
\c - - - :worker_2_port

DROP TRIGGER reject_bad ON objects_1200003;

CREATE CONSTRAINT TRIGGER reject_bad
AFTER INSERT ON objects_1200003
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad();

\c - - - :master_port

-- should be the same story as before, just at COMMIT time
BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO objects VALUES (2, 'BAD');
INSERT INTO labs VALUES (9, 'Umbrella Corporation');
COMMIT;

-- data should be persisted
SELECT * FROM objects WHERE id = 2;
SELECT * FROM labs WHERE id = 7;

-- but one placement should be bad
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.nodename = 'localhost'
AND    sp.nodeport = :worker_2_port
AND    sp.shardstate = 3
AND    s.logicalrelid = 'objects'::regclass;

DELETE FROM objects;

-- mark shards as healthy again; delete all data
UPDATE pg_dist_shard_placement AS sp SET shardstate = 1
FROM   pg_dist_shard AS s
WHERE  sp.shardid = s.shardid
AND    s.logicalrelid = 'objects'::regclass;

-- what if all nodes have failures at COMMIT time?
\c - - - :worker_1_port

DROP TRIGGER reject_bad ON labs_1200002;

CREATE CONSTRAINT TRIGGER reject_bad
AFTER INSERT ON labs_1200002
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad();

\c - - - :master_port

BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO objects VALUES (2, 'BAD');
INSERT INTO labs VALUES (8, 'Aperture Science');
INSERT INTO labs VALUES (9, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects WHERE id = 1;
SELECT * FROM labs WHERE id = 8;

-- all placements should remain healthy
SELECT count(*)
FROM   pg_dist_shard_placement AS sp,
	   pg_dist_shard           AS s
WHERE  sp.shardid = s.shardid
AND    sp.shardstate = 1
AND    (s.logicalrelid = 'objects'::regclass OR
	    s.logicalrelid = 'labs'::regclass);

-- what if one shard (objects) succeeds but another (labs) completely fails?
\c - - - :worker_2_port

DROP TRIGGER reject_bad ON objects_1200003;

\c - - - :master_port

BEGIN;
INSERT INTO objects VALUES (1, 'apple');
INSERT INTO labs VALUES (8, 'Aperture Science');
INSERT INTO labs VALUES (9, 'BAD');
COMMIT;

-- data to objects should be persisted, but labs should not...
SELECT * FROM objects WHERE id = 1;
SELECT * FROM labs WHERE id = 8;

-- labs should be healthy, but one object placement shouldn't be
SELECT   s.logicalrelid::regclass::text, sp.shardstate, count(*)
FROM     pg_dist_shard_placement AS sp,
	     pg_dist_shard           AS s
WHERE    sp.shardid = s.shardid
AND      (s.logicalrelid = 'objects'::regclass OR
	      s.logicalrelid = 'labs'::regclass)
GROUP BY s.logicalrelid, sp.shardstate
ORDER BY s.logicalrelid, sp.shardstate;

-- some append-partitioned tests for good measure
CREATE TABLE append_researchers ( LIKE researchers );

SELECT master_create_distributed_table('append_researchers', 'id', 'append');

SET citus.shard_replication_factor TO 1;

SELECT master_create_empty_shard('append_researchers') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 0, shardmaxvalue = 500000
WHERE shardid = :new_shard_id;

SELECT master_create_empty_shard('append_researchers') AS new_shard_id
\gset
UPDATE pg_dist_shard SET shardminvalue = 500000, shardmaxvalue = 1000000
WHERE shardid = :new_shard_id;

SET citus.shard_replication_factor TO DEFAULT;

-- try single-shard INSERT
BEGIN;
INSERT INTO append_researchers VALUES (0, 0, 'John Backus');
COMMIT;

SELECT * FROM append_researchers WHERE id = 0;

-- try rollback
BEGIN;
DELETE FROM append_researchers WHERE id = 0;
ROLLBACK;

SELECT * FROM append_researchers WHERE id = 0;

-- try hitting shard on other node
BEGIN;
INSERT INTO append_researchers VALUES (1, 1, 'John McCarthy');
INSERT INTO append_researchers VALUES (500000, 500000, 'Tony Hoare');
ROLLBACK;

SELECT * FROM append_researchers;
