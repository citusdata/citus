
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

-- might be confusing to have two people in the same lab with the same name
CREATE UNIQUE INDEX avoid_name_confusion_idx ON researchers (lab_id, name);

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

-- trigger a unique constraint violation
BEGIN;
UPDATE researchers SET name = 'John Backus' WHERE id = 1 AND lab_id = 1;
ABORT;

-- creating savepoints should work...
BEGIN;
INSERT INTO researchers VALUES (5, 3, 'Dennis Ritchie');
SAVEPOINT hire_thompson;
INSERT INTO researchers VALUES (6, 3, 'Ken Thompson');
COMMIT;

SELECT name FROM researchers WHERE lab_id = 3 AND id = 6;

-- even if created by PL/pgSQL...
\set VERBOSITY terse
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
	INSERT INTO researchers VALUES (11, 11, 'Whitfield Diffie');
	INSERT INTO researchers VALUES (NULL, 10, 'Edsger Dijkstra');
EXCEPTION
    WHEN not_null_violation THEN
        RAISE NOTICE 'caught not_null_violation';
END $$;
COMMIT;
\set VERBOSITY default


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

-- SELECTs may occur after a modification: First check that selecting
-- from the modified node works.
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
SELECT count(*) FROM researchers WHERE lab_id = 6;
ABORT;

-- then check that SELECT going to new node still is fine
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

-- applies to DDL, too
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
ALTER TABLE labs ADD COLUMN motto text;
COMMIT;

-- whether it occurs first or second
BEGIN;
ALTER TABLE labs ADD COLUMN motto text;
INSERT INTO labs VALUES (6, 'Bell Labs');
COMMIT;

-- but the DDL should correctly roll back
\d labs
SELECT * FROM labs WHERE id = 6;

-- COPY can't happen second,
BEGIN;
INSERT INTO labs VALUES (6, 'Bell Labs');
\copy labs from stdin delimiter ','
10,Weyland-Yutani
\.
COMMIT;

-- though it will work if before any modifications
BEGIN;
\copy labs from stdin delimiter ','
10,Weyland-Yutani
\.
SELECT name FROM labs WHERE id = 10;
INSERT INTO labs VALUES (6, 'Bell Labs');
COMMIT;

-- but a double-copy isn't allowed (the first will persist)
BEGIN;
\copy labs from stdin delimiter ','
11,Planet Express
\.
\copy labs from stdin delimiter ','
12,fsociety
\.
COMMIT;

SELECT name FROM labs WHERE id = 11;

-- finally, ALTER and copy aren't compatible
BEGIN;
ALTER TABLE labs ADD COLUMN motto text;
\copy labs from stdin delimiter ','
12,fsociety,lol
\.
COMMIT;

-- but the DDL should correctly roll back
\d labs
SELECT * FROM labs WHERE id = 12;

-- and if the copy is before the ALTER...
BEGIN;
\copy labs from stdin delimiter ','
12,fsociety
\.
ALTER TABLE labs ADD COLUMN motto text;
COMMIT;

-- the DDL fails, but copy persists
\d labs
SELECT * FROM labs WHERE id = 12;

-- multi-shard operations can co-exist with DDL in a transactional way
BEGIN;
ALTER TABLE labs ADD COLUMN motto text;
SELECT master_modify_multiple_shards('DELETE FROM labs');
ALTER TABLE labs ADD COLUMN score float;
ROLLBACK;

-- should have rolled everything back
SELECT * FROM labs WHERE id = 12;

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
\set VERBOSITY terse
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
\set VERBOSITY default

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

-- we use 2PC for reference tables by default
-- let's add some tests for them
CREATE TABLE reference_modifying_xacts (key int, value int);
SELECT create_reference_table('reference_modifying_xacts');

-- very basic test, ensure that INSERTs work
INSERT INTO reference_modifying_xacts VALUES (1, 1);
SELECT * FROM reference_modifying_xacts;

-- now ensure that it works in a transaction as well
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (2, 2);
SELECT * FROM reference_modifying_xacts;
COMMIT;

-- we should be able to see the insert outside of the transaction as well
SELECT * FROM reference_modifying_xacts;

-- rollback should also work
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (3, 3);
SELECT * FROM reference_modifying_xacts;
ROLLBACK;

-- see that we've not inserted
SELECT * FROM reference_modifying_xacts;

-- lets fail on of the workers at before the commit time
\c - - - :worker_1_port

CREATE FUNCTION reject_bad_reference() RETURNS trigger AS $rb$
    BEGIN
        IF (NEW.key = 999) THEN
            RAISE 'illegal value';
        END IF;

        RETURN NEW;
    END;
$rb$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER reject_bad_reference
AFTER INSERT ON reference_modifying_xacts_1200006
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad_reference();

\c - - - :master_port
\set VERBOSITY terse
-- try without wrapping inside a transaction
INSERT INTO reference_modifying_xacts VALUES (999, 3);

-- same test within a transaction
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (999, 3);
COMMIT;

-- lets fail one of the workers at COMMIT time
\c - - - :worker_1_port
DROP TRIGGER reject_bad_reference ON reference_modifying_xacts_1200006;

CREATE CONSTRAINT TRIGGER reject_bad_reference
AFTER INSERT ON reference_modifying_xacts_1200006
DEFERRABLE INITIALLY  DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad_reference();

\c - - - :master_port
\set VERBOSITY terse

-- try without wrapping inside a transaction
INSERT INTO reference_modifying_xacts VALUES (999, 3);

-- same test within a transaction
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (999, 3);
COMMIT;

-- all placements should be healthy
SELECT   s.logicalrelid::regclass::text, sp.shardstate, count(*)
FROM     pg_dist_shard_placement AS sp,
	     pg_dist_shard           AS s
WHERE    sp.shardid = s.shardid
AND      s.logicalrelid = 'reference_modifying_xacts'::regclass
GROUP BY s.logicalrelid, sp.shardstate
ORDER BY s.logicalrelid, sp.shardstate;

-- for the time-being drop the constraint
\c - - - :worker_1_port
DROP TRIGGER reject_bad_reference ON reference_modifying_xacts_1200006;


\c - - - :master_port

-- now create a hash distributed table and run tests
-- including both the reference table and the hash
-- distributed table
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;
CREATE TABLE hash_modifying_xacts (key int, value int);
SELECT create_distributed_table('hash_modifying_xacts', 'key');

-- let's try to expand the xact participants
BEGIN;
INSERT INTO hash_modifying_xacts VALUES (1, 1);
INSERT INTO reference_modifying_xacts VALUES (10, 10);
COMMIT;

-- lets fail one of the workers before COMMIT time for the hash table
\c - - - :worker_1_port

CREATE FUNCTION reject_bad_hash() RETURNS trigger AS $rb$
    BEGIN
        IF (NEW.key = 997) THEN
            RAISE 'illegal value';
        END IF;

        RETURN NEW;
    END;
$rb$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER reject_bad_hash
AFTER INSERT ON hash_modifying_xacts_1200007
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad_hash();

\c - - - :master_port
\set VERBOSITY terse

-- the transaction as a whole should fail
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (55, 10);
INSERT INTO hash_modifying_xacts VALUES (997, 1);
COMMIT;

-- ensure that the value didn't go into the reference table
SELECT * FROM reference_modifying_xacts WHERE key = 55;

-- now lets fail on of the workers for the hash distributed table table
-- when there is a reference table involved
\c - - - :worker_1_port
DROP TRIGGER reject_bad_hash ON hash_modifying_xacts_1200007;

-- the trigger is on execution time
CREATE CONSTRAINT TRIGGER reject_bad_hash
AFTER INSERT ON hash_modifying_xacts_1200007
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE PROCEDURE reject_bad_hash();

\c - - - :master_port
\set VERBOSITY terse

-- the transaction as a whole should fail
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (12, 12);
INSERT INTO hash_modifying_xacts VALUES (997, 1);
COMMIT;

-- ensure that the values didn't go into the reference table
SELECT * FROM reference_modifying_xacts WHERE key = 12;

-- all placements should be healthy
SELECT   s.logicalrelid::regclass::text, sp.shardstate, count(*)
FROM     pg_dist_shard_placement AS sp,
	     pg_dist_shard           AS s
WHERE    sp.shardid = s.shardid
AND      (s.logicalrelid = 'reference_modifying_xacts'::regclass OR 
		  s.logicalrelid = 'hash_modifying_xacts'::regclass)	
GROUP BY s.logicalrelid, sp.shardstate
ORDER BY s.logicalrelid, sp.shardstate;

-- now, fail the insert on reference table
-- and ensure that hash distributed table's
-- change is rollbacked as well

\c - - - :worker_1_port

CREATE CONSTRAINT TRIGGER reject_bad_reference
AFTER INSERT ON reference_modifying_xacts_1200006
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE PROCEDURE reject_bad_reference();

\c - - - :master_port
\set VERBOSITY terse

BEGIN;

-- to expand participant to include all worker nodes
INSERT INTO reference_modifying_xacts VALUES (66, 3);
INSERT INTO hash_modifying_xacts VALUES (80, 1);
INSERT INTO reference_modifying_xacts VALUES (999, 3);
COMMIT;

SELECT * FROM hash_modifying_xacts WHERE key = 80;
SELECT * FROM reference_modifying_xacts WHERE key = 66;
SELECT * FROM reference_modifying_xacts WHERE key = 999;

-- all placements should be healthy
SELECT   s.logicalrelid::regclass::text, sp.shardstate, count(*)
FROM     pg_dist_shard_placement AS sp,
	     pg_dist_shard           AS s
WHERE    sp.shardid = s.shardid
AND      (s.logicalrelid = 'reference_modifying_xacts'::regclass OR 
		  s.logicalrelid = 'hash_modifying_xacts'::regclass)	
GROUP BY s.logicalrelid, sp.shardstate
ORDER BY s.logicalrelid, sp.shardstate;

-- now show that all modifications to reference
-- tables are done in 2PC
SELECT recover_prepared_transactions();

INSERT INTO reference_modifying_xacts VALUES (70, 70);
SELECT count(*) FROM pg_dist_transaction;

-- reset the transactions table
SELECT recover_prepared_transactions();
BEGIN;
INSERT INTO reference_modifying_xacts VALUES (71, 71);
COMMIT;
SELECT count(*) FROM pg_dist_transaction;


-- create a hash distributed tablw which spans all nodes
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 2;
CREATE TABLE hash_modifying_xacts_second (key int, value int);
SELECT create_distributed_table('hash_modifying_xacts_second', 'key');

-- reset the transactions table
SELECT recover_prepared_transactions();

BEGIN;
INSERT INTO hash_modifying_xacts_second VALUES (72, 1);
INSERT INTO reference_modifying_xacts VALUES (72, 3);
COMMIT;
SELECT count(*) FROM pg_dist_transaction;

-- reset the transactions table
SELECT recover_prepared_transactions();
DELETE FROM reference_modifying_xacts;
SELECT count(*) FROM pg_dist_transaction;

-- reset the transactions table
SELECT recover_prepared_transactions();
UPDATE reference_modifying_xacts SET key = 10;
SELECT count(*) FROM pg_dist_transaction;

-- now to one more type of failure testing
-- in which we'll make the remote host unavailable

-- first create the new user on all nodes
CREATE USER test_user;
\c - - - :worker_1_port
CREATE USER test_user;
\c - - - :worker_2_port
CREATE USER test_user;

-- now connect back to the master with the new user
\c - test_user - :master_port
CREATE TABLE reference_failure_test (key int, value int);
SELECT create_reference_table('reference_failure_test');

-- ensure that the shard is created for this user
\c - test_user - :worker_1_port
\dt reference_failure_test_1200015

-- now connect with the default user, 
-- and rename the existing user
\c - :default_user - :worker_1_port
ALTER USER test_user RENAME TO test_user_new;

-- connect back to master and query the reference table
 \c - test_user - :master_port
-- should fail since the worker doesn't have test_user anymore
INSERT INTO reference_failure_test VALUES (1, '1');

-- the same as the above, but wrapped within a transaction
BEGIN;
INSERT INTO reference_failure_test VALUES (1, '1');
COMMIT;

-- show that no data go through the table and shard states are good
SELECT * FROM reference_failure_test;

-- all placements should be healthy
SELECT   s.logicalrelid::regclass::text, sp.shardstate, count(*)
FROM     pg_dist_shard_placement AS sp,
	     pg_dist_shard           AS s
WHERE    sp.shardid = s.shardid
AND      s.logicalrelid = 'reference_failure_test'::regclass	
GROUP BY s.logicalrelid, sp.shardstate
ORDER BY s.logicalrelid, sp.shardstate;

-- connect back to the worker and set rename the test_user back
\c - :default_user - :worker_1_port
ALTER USER test_user_new RENAME TO test_user;

-- connect back to the master with the proper user to continue the tests 
\c - :default_user - :master_port
DROP TABLE reference_modifying_xacts, hash_modifying_xacts, hash_modifying_xacts_second;
