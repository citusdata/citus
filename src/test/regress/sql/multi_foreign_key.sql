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

-- test cascading truncate
INSERT INTO referenced_table VALUES(2, 2);
INSERT INTO referencing_table VALUES(2, 2);
TRUNCATE referenced_table CASCADE;
SELECT * FROM referencing_table;

-- drop table for next tests
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint options
-- test ON DELETE CASCADE
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE CASCADE);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');

-- single shard cascading delete
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
DELETE FROM referenced_table WHERE id = 1;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;

-- multi shard cascading delete
INSERT INTO referenced_table VALUES(2, 2);
INSERT INTO referencing_table VALUES(2, 2);
SELECT master_modify_multiple_shards('DELETE FROM referenced_table');
SELECT * FROM referencing_table;

-- multi shard cascading delete with alter table
INSERT INTO referenced_table VALUES(3, 3);
INSERT INTO referencing_table VALUES(3, 3);
BEGIN;
ALTER TABLE referencing_table ADD COLUMN x int DEFAULT 0;
SELECT master_modify_multiple_shards('DELETE FROM referenced_table');
COMMIT;

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


-- Similar tests, but this time we push foreign key constraints created by ALTER TABLE queries
-- create tables
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
SELECT master_create_distributed_table('referenced_table', 'id', 'hash');
SELECT master_create_worker_shards('referenced_table', 4, 1);

CREATE TABLE referencing_table(id int, ref_id int);
SELECT master_create_distributed_table('referencing_table', 'ref_id', 'hash');
SELECT master_create_worker_shards('referencing_table', 4, 1);


-- test foreign constraint creation
-- test foreign constraint creation with not supported parameters
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL;
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET DEFAULT;
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE CASCADE;

-- test foreign constraint creation with multiple subcommands
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id),
							  ADD CONSTRAINT test_constraint FOREIGN KEY(id) REFERENCES referenced_table(test_column);

-- test foreign constraint creation without giving explicit name
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id);

-- test foreign constraint creation on NOT co-located tables
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id);

-- create co-located tables
DROP TABLE referencing_table;
DROP TABLE referenced_table;
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');

-- test foreign constraint creation on non-partition columns
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(id) REFERENCES referenced_table(id);

-- test foreign constraint creation while column list are in incorrect order
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column);

-- test foreign constraint creation while column list are not in same length
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id, test_column);

-- test foreign constraint creation while existing tables does not satisfy the constraint
INSERT INTO referencing_table VALUES(1, 1);
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id);

-- test foreign constraint with correct conditions
DELETE FROM referencing_table WHERE ref_id = 1;
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id);


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


-- test DROP CONSTRAINT
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;


-- test foreign constraint options
-- test ON DELETE CASCADE
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE CASCADE;
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
DELETE FROM referenced_table WHERE id = 1;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test ON DELETE NO ACTION + DEFERABLE + INITIALLY DEFERRED
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED;
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
DELETE FROM referenced_table WHERE id = 1;
BEGIN;
DELETE FROM referenced_table WHERE id = 1;
DELETE FROM referencing_table WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test ON DELETE RESTRICT
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE RESTRICT;
INSERT INTO referenced_table VALUES(1, 1);
INSERT INTO referencing_table VALUES(1, 1);
BEGIN;
DELETE FROM referenced_table WHERE id = 1;
DELETE FROM referencing_table WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test ON UPDATE NO ACTION + DEFERABLE + INITIALLY DEFERRED
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED;
UPDATE referenced_table SET test_column = 10 WHERE id = 1;
BEGIN;
UPDATE referenced_table SET test_column = 10 WHERE id = 1;
UPDATE referencing_table SET id = 10 WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test ON UPDATE RESTRICT
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE RESTRICT;
BEGIN;
UPDATE referenced_table SET test_column = 20 WHERE id = 1;
UPDATE referencing_table SET id = 20 WHERE ref_id = 1;
COMMIT;
SELECT * FROM referencing_table;
SELECT * FROM referenced_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test MATCH SIMPLE
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH SIMPLE;
INSERT INTO referencing_table VALUES(null, 2);
SELECT * FROM referencing_table;
DELETE FROM referencing_table WHERE ref_id = 2;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- test MATCH FULL
ALTER TABLE referencing_table ADD CONSTRAINT test_constraint FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH FULL;
INSERT INTO referencing_table VALUES(null, 2);
SELECT * FROM referencing_table;
ALTER TABLE referencing_table DROP CONSTRAINT test_constraint;

-- we no longer need those tables
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test cyclical foreign keys
CREATE TABLE cyclic_reference_table1(id int, table2_id int, PRIMARY KEY(id, table2_id));
CREATE TABLE cyclic_reference_table2(id int, table1_id int, PRIMARY KEY(id, table1_id));
SELECT create_distributed_table('cyclic_reference_table1', 'id', 'hash');
SELECT create_distributed_table('cyclic_reference_table2', 'table1_id', 'hash');
ALTER TABLE cyclic_reference_table1 ADD CONSTRAINT cyclic_constraint1 FOREIGN KEY(id, table2_id) REFERENCES cyclic_reference_table2(table1_id, id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE cyclic_reference_table2 ADD CONSTRAINT cyclic_constraint2 FOREIGN KEY(id, table1_id) REFERENCES cyclic_reference_table1(table2_id, id) DEFERRABLE INITIALLY DEFERRED;

-- test insertion to a table which has cyclic foreign constraints, we expect that to fail
INSERT INTO cyclic_reference_table1 VALUES(1, 1);

-- proper insertion to table with cyclic dependency
BEGIN;
INSERT INTO cyclic_reference_table1 VALUES(1, 1);
INSERT INTO cyclic_reference_table2 VALUES(1, 1);
COMMIT;

-- verify that rows are actually inserted
SELECT * FROM cyclic_reference_table1;
SELECT * FROM cyclic_reference_table2;

-- test dropping cyclic referenced tables
-- we expect those two queries to fail
DROP TABLE cyclic_reference_table1;
DROP TABLE cyclic_reference_table2;

-- proper way of DROP with CASCADE option
DROP TABLE cyclic_reference_table1 CASCADE;
DROP TABLE cyclic_reference_table2 CASCADE;

-- test creation of foreign keys in a transaction
CREATE TABLE transaction_referenced_table(id int PRIMARY KEY);
CREATE TABLE transaction_referencing_table(id int, ref_id int);

BEGIN;
ALTER TABLE transaction_referencing_table ADD CONSTRAINT transaction_fk_constraint FOREIGN KEY(ref_id) REFERENCES transaction_referenced_table(id);
COMMIT;

-- test insertion to referencing table, we expect that to fail
INSERT INTO transaction_referencing_table VALUES(1, 1);

-- proper insertion to both referenced and referencing tables
INSERT INTO transaction_referenced_table VALUES(1);
INSERT INTO transaction_referencing_table VALUES(1, 1);

-- verify that rows are actually inserted
SELECT * FROM transaction_referenced_table;
SELECT * FROM transaction_referencing_table;

-- we no longer need those tables
DROP TABLE transaction_referencing_table;
DROP TABLE transaction_referenced_table;

-- test self referencing foreign key
CREATE TABLE self_referencing_table1(
    id int,
    other_column int,
    other_column_ref int,
    PRIMARY KEY(id, other_column),
    FOREIGN KEY(id, other_column_ref) REFERENCES self_referencing_table1(id, other_column)
);
SELECT create_distributed_table('self_referencing_table1', 'id', 'hash');

-- test insertion to self referencing table
INSERT INTO self_referencing_table1 VALUES(1, 1, 1);
-- we expect this query to fail
INSERT INTO self_referencing_table1 VALUES(1, 2, 3);

-- verify that rows are actually inserted
SELECT * FROM self_referencing_table1;

-- we no longer need those tables
DROP TABLE self_referencing_table1;

-- test self referencing foreign key with ALTER TABLE
CREATE TABLE self_referencing_table2(id int, other_column int, other_column_ref int, PRIMARY KEY(id, other_column));
SELECT create_distributed_table('self_referencing_table2', 'id', 'hash');
ALTER TABLE self_referencing_table2 ADD CONSTRAINT self_referencing_fk_constraint FOREIGN KEY(id, other_column_ref) REFERENCES self_referencing_table2(id, other_column);

-- test insertion to self referencing table
INSERT INTO self_referencing_table2 VALUES(1, 1, 1);
-- we expect this query to fail
INSERT INTO self_referencing_table2 VALUES(1, 2, 3);

-- verify that rows are actually inserted
SELECT * FROM self_referencing_table2;

-- we no longer need those tables
DROP TABLE self_referencing_table2;


-- test reference tables
-- test foreign key creation on CREATE TABLE from reference table
CREATE TABLE referenced_by_reference_table(id int PRIMARY KEY, other_column int);
SELECT create_distributed_table('referenced_by_reference_table', 'id');

CREATE TABLE reference_table(id int, referencing_column int REFERENCES referenced_by_reference_table(id));
SELECT create_reference_table('reference_table');

-- test foreign key creation on CREATE TABLE to reference table
DROP TABLE reference_table;
CREATE TABLE reference_table(id int PRIMARY KEY, referencing_column int);
SELECT create_reference_table('reference_table');

CREATE TABLE references_to_reference_table(id int, referencing_column int REFERENCES reference_table(id));
SELECT create_distributed_table('references_to_reference_table', 'referencing_column');

-- test foreign key creation on CREATE TABLE from + to reference table
CREATE TABLE reference_table2(id int, referencing_column int REFERENCES reference_table(id));
SELECT create_reference_table('reference_table2');

-- test foreign key creation on ALTER TABLE from reference table
ALTER TABLE reference_table ADD CONSTRAINT fk FOREIGN KEY(referencing_column) REFERENCES referenced_by_reference_table(id);

-- test foreign key creation on ALTER TABLE to reference table
DROP TABLE references_to_reference_table;
CREATE TABLE references_to_reference_table(id int, referencing_column int);
SELECT create_distributed_table('references_to_reference_table', 'referencing_column');
ALTER TABLE references_to_reference_table ADD CONSTRAINT fk FOREIGN KEY(referencing_column) REFERENCES reference_table(id);

-- test foreign key creation on ALTER TABLE from + to reference table
DROP TABLE reference_table2;
CREATE TABLE reference_table2(id int, referencing_column int);
SELECT create_reference_table('reference_table2');
ALTER TABLE reference_table2 ADD CONSTRAINT fk FOREIGN KEY(referencing_column) REFERENCES reference_table(id);

-- we no longer need those tables
DROP TABLE referenced_by_reference_table, references_to_reference_table, reference_table, reference_table2;
