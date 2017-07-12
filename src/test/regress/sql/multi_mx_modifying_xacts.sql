
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1340000;


-- ===================================================================
-- test end-to-end modification functionality for mx tables in transactions
-- ===================================================================

-- add some data
INSERT INTO researchers_mx VALUES (1, 1, 'Donald Knuth');
INSERT INTO researchers_mx VALUES (2, 1, 'Niklaus Wirth');
INSERT INTO researchers_mx VALUES (3, 2, 'Tony Hoare');
INSERT INTO researchers_mx VALUES (4, 2, 'Kenneth Iverson');

-- replace a researcher, reusing their id on the coordinator
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 2;
INSERT INTO researchers_mx VALUES (2, 1, 'John Backus');
COMMIT;
SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 2;

-- do it on the worker node as well
\c - - - :worker_1_port
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 2;
INSERT INTO researchers_mx VALUES (2, 1, 'John Backus Worker 1');
COMMIT;
SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 2;

-- do it on the worker other node as well
\c - - - :worker_2_port
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 2;
INSERT INTO researchers_mx VALUES (2, 1, 'John Backus Worker 2');
COMMIT;
SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 2;


\c - - - :master_port

-- abort a modification
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 1;
ABORT;

SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 1;

\c - - - :worker_1_port

-- abort a modification on the worker node
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 1;
ABORT;

SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 1;

\c - - - :worker_2_port

-- abort a modification on the other worker node
BEGIN;
DELETE FROM researchers_mx WHERE lab_id = 1 AND id = 1;
ABORT;

SELECT name FROM researchers_mx WHERE lab_id = 1 AND id = 1;


-- switch back to the first worker node
\c - - - :worker_1_port

-- creating savepoints should work...
BEGIN;
INSERT INTO researchers_mx VALUES (5, 3, 'Dennis Ritchie');
SAVEPOINT hire_thompson;
INSERT INTO researchers_mx VALUES (6, 3, 'Ken Thompson');
COMMIT;

SELECT name FROM researchers_mx WHERE lab_id = 3 AND id = 6;

-- even if created by PL/pgSQL...
\set VERBOSITY terse
BEGIN;
DO $$
BEGIN
    INSERT INTO researchers_mx VALUES (10, 10, 'Edsger Dijkstra');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;

-- but rollback should not
BEGIN;
INSERT INTO researchers_mx VALUES (7, 4, 'Jim Gray');
SAVEPOINT hire_engelbart;
INSERT INTO researchers_mx VALUES (8, 4, 'Douglas Engelbart');
ROLLBACK TO hire_engelbart;
COMMIT;

SELECT name FROM researchers_mx WHERE lab_id = 4;

BEGIN;
DO $$
BEGIN
    INSERT INTO researchers_mx VALUES (NULL, 10, 'Edsger Dijkstra');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;
\set VERBOSITY default


-- should be valid to edit labs_mx after researchers_mx...
BEGIN;
INSERT INTO researchers_mx VALUES (8, 5, 'Douglas Engelbart');
INSERT INTO labs_mx VALUES (5, 'Los Alamos');
COMMIT;

SELECT * FROM researchers_mx, labs_mx WHERE labs_mx.id = researchers_mx.lab_id;

-- but not the other way around (would require expanding xact participants)...
BEGIN;
INSERT INTO labs_mx VALUES (6, 'Bell labs_mx');
INSERT INTO researchers_mx VALUES (9, 6, 'Leslie Lamport');
COMMIT;

-- have the same test on the other worker node
\c - - - :worker_2_port
-- should be valid to edit labs_mx after researchers_mx...
BEGIN;
INSERT INTO researchers_mx VALUES (8, 5, 'Douglas Engelbart');
INSERT INTO labs_mx VALUES (5, 'Los Alamos');
COMMIT;

SELECT * FROM researchers_mx, labs_mx WHERE labs_mx.id = researchers_mx.lab_id;

-- but not the other way around (would require expanding xact participants)...
BEGIN;
INSERT INTO labs_mx VALUES (6, 'Bell labs_mx');
INSERT INTO researchers_mx VALUES (9, 6, 'Leslie Lamport');
COMMIT;

-- switch back to the worker node
\c - - - :worker_1_port

-- this logic doesn't apply to router SELECTs occurring after a modification:
-- selecting from the modified node is fine...
BEGIN;
INSERT INTO labs_mx VALUES (6, 'Bell labs_mx');
SELECT count(*) FROM researchers_mx WHERE lab_id = 6;
ABORT;

-- doesn't apply to COPY after modifications
BEGIN;
INSERT INTO labs_mx VALUES (6, 'Bell labs_mx');
\copy labs_mx from stdin delimiter ','
10,Weyland-Yutani-1
\.
COMMIT;

-- copy will also work if before any modifications
BEGIN;
\copy labs_mx from stdin delimiter ','
10,Weyland-Yutani-2
\.
SELECT name FROM labs_mx WHERE id = 10;
INSERT INTO labs_mx VALUES (6, 'Bell labs_mx');
COMMIT;

\c - - - :worker_1_port
-- test primary key violations
BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (1, 'orange');
COMMIT;

-- data shouldn't have persisted...
SELECT * FROM objects_mx WHERE id = 1;

-- same test on the second worker node
\c - - - :worker_2_port
-- test primary key violations
BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (1, 'orange');
COMMIT;

-- data shouldn't have persisted...
SELECT * FROM objects_mx WHERE id = 1;

-- create trigger on one worker to reject certain values
\c - - - :worker_1_port

CREATE FUNCTION reject_bad_mx() RETURNS trigger AS $rb$
    BEGIN
        IF (NEW.name = 'BAD') THEN
            RAISE 'illegal value';
        END IF;

        RETURN NEW;
    END;
$rb$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER reject_bad_mx
AFTER INSERT ON objects_mx_1220103
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad_mx();

-- test partial failure; statement 1 successed, statement 2 fails
\set VERBOSITY terse
BEGIN;
INSERT INTO labs_mx VALUES (7, 'E Corp');
INSERT INTO objects_mx VALUES (2, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 2;
SELECT * FROM labs_mx WHERE id = 7;

-- same failure test from worker 2
\c - - - :worker_2_port

-- test partial failure; statement 1 successed, statement 2 fails
BEGIN;
INSERT INTO labs_mx VALUES (7, 'E Corp');
INSERT INTO objects_mx VALUES (2, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 2;
SELECT * FROM labs_mx WHERE id = 7;

\c - - - :worker_1_port

-- what if there are errors on different shards at different times?
\c - - - :worker_1_port

CREATE CONSTRAINT TRIGGER reject_bad_mx
AFTER INSERT ON labs_mx_1220102
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad_mx();

BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (2, 'BAD');
INSERT INTO labs_mx VALUES (8, 'Aperture Science');
INSERT INTO labs_mx VALUES (9, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 1;
SELECT * FROM labs_mx WHERE id = 8;

-- same test from the other worker
\c - - - :worker_2_port


BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (2, 'BAD');
INSERT INTO labs_mx VALUES (8, 'Aperture Science');
INSERT INTO labs_mx VALUES (9, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 1;
SELECT * FROM labs_mx WHERE id = 8;


-- what if the failures happen at COMMIT time?
\c - - - :worker_1_port

DROP TRIGGER reject_bad_mx ON objects_mx_1220103;

CREATE CONSTRAINT TRIGGER reject_bad_mx
AFTER INSERT ON objects_mx_1220103
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad_mx();

-- should be the same story as before, just at COMMIT time
BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (2, 'BAD');
INSERT INTO labs_mx VALUES (9, 'Umbrella Corporation');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 2;
SELECT * FROM labs_mx WHERE id = 7;


DROP TRIGGER reject_bad_mx ON labs_mx_1220102;

CREATE CONSTRAINT TRIGGER reject_bad_mx
AFTER INSERT ON labs_mx_1220102
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad_mx();

BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO objects_mx VALUES (2, 'BAD');
INSERT INTO labs_mx VALUES (8, 'Aperture Science');
INSERT INTO labs_mx VALUES (9, 'BAD');
COMMIT;

-- data should NOT be persisted
SELECT * FROM objects_mx WHERE id = 1;
SELECT * FROM labs_mx WHERE id = 8;

-- what if one shard (objects_mx) succeeds but another (labs_mx) completely fails?
\c - - - :worker_1_port

DROP TRIGGER reject_bad_mx ON objects_mx_1220103;

BEGIN;
INSERT INTO objects_mx VALUES (1, 'apple');
INSERT INTO labs_mx VALUES (8, 'Aperture Science');
INSERT INTO labs_mx VALUES (9, 'BAD');
COMMIT;

-- no data should persists
SELECT * FROM objects_mx WHERE id = 1;
SELECT * FROM labs_mx WHERE id = 8;
