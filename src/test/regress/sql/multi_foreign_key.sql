--
-- MULTI_FOREIGN_KEY
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1350000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1350000;

-- set shard_count to 4 for faster tests, because we create/drop lots of shards in this test.
SET citus.shard_count TO 4;

-- create tables
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
SELECT create_distributed_table('referenced_table', 'id', 'hash');

-- test foreign constraint creation with not supported parameters
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET DEFAULT);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE CASCADE);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

-- test foreign constraint creation on NOT co-located tables
SET citus.shard_count TO 8;
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id));
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;
SET citus.shard_count TO 4;

-- test foreign constraint creation on non-partition columns
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id) REFERENCES referenced_table(id));
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

-- test foreign constraint creation while column list are in incorrect order
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column));
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;

-- test foreign constraint with replication factor > 1
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id));
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint with correct conditions
SET citus.shard_replication_factor TO 1;
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id));
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');


-- test inserts
-- test insert to referencing table while there is NO corresponding value in referenced table
INSERT INTO referencing_table VALUES(1, 1);

-- test insert to referencing while there is corresponding value in referenced table
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);


-- test deletes
-- test delete from referenced table while there is corresponding value in referencing table
DELETE FROM referenced_table WHERE id = 1;

-- test delete from referenced table while there is NO corresponding value in referencing table
DELETE FROM referencing_table WHERE ref_id = 1;
DELETE FROM referenced_table WHERE id = 1;

-- drop table for next tests
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint options
-- test ON DELETE CASCADE
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE CASCADE);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
DELETE FROM referenced_table WHERE id = 1;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test ON DELETE NO ACTION + DEFERABLE + INITIALLY DEFERRED
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
DELETE FROM referenced_table WHERE id = 1;
BEGIN;
DELETE FROM referenced_table WHERE id = 1;
DELETE FROM referencing_table WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test ON DELETE RESTRICT
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE RESTRICT);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
BEGIN;
DELETE FROM referenced_table WHERE id = 1;
DELETE FROM referencing_table WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test ON UPDATE NO ACTION + DEFERABLE + INITIALLY DEFERRED
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
UPDATE referenced_table SET test_column = 10 WHERE id = 1;
BEGIN;
UPDATE referenced_table SET test_column = 10 WHERE id = 1;
UPDATE referencing_table SET id = 10 WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test ON UPDATE RESTRICT
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE RESTRICT);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
BEGIN;
UPDATE referenced_table SET test_column = 20 WHERE id = 1;
UPDATE referencing_table SET id = 20 WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test MATCH SIMPLE
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH SIMPLE);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referencing_table VALUES(null, 2);
SELECT * FROM referencing_table;
DELETE FROM referencing_table WHERE ref_id = 2;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test MATCH FULL
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH FULL);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
INSERT INTO referencing_table VALUES(null, 2);
SELECT * FROM referencing_table;
DROP TABLE referencing_table;
DROP TABLE referenced_table;
