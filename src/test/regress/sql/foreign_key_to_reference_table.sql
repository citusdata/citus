SET citus.shard_replication_factor TO 1;
CREATE SCHEMA fkey_reference_table;
SET search_path TO 'fkey_reference_table';

CREATE VIEW num_of_foreign_keys AS
SELECT (run_command_on_workers($$
  SELECT
      count(tc.constraint_name)
  FROM
      information_schema.table_constraints AS tc
  WHERE constraint_type = 'FOREIGN KEY' AND tc.constraint_name LIKE 'fkey_ref%'
$$)).*;

CREATE TABLE referenced_table(id int UNIQUE, test_column int);
SELECT create_reference_table('referenced_table');

-- we still do not support update/delete operations through foreign constraints if the foreign key includes the distribution column
-- All should fail
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
DROP TABLE referencing_table;

-- try with multiple columns including the distribution column
DROP TABLE referenced_table;
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id, test_column));
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE SET DEFAULT;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE CASCADE;
DROP TABLE referencing_table;

-- all of the above is supported if the foreign key does not include distribution column
DROP TABLE referenced_table;
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id));
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET NULL;
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE SET DEFAULT;
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE CASCADE;
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;

-- foreign keys are only supported when the replication factor = 1
SET citus.shard_replication_factor TO 2;
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(id);
SELECT * FROM num_of_foreign_keys;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint with correct conditions
SET citus.shard_replication_factor TO 1;
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(id);


-- test inserts
-- test insert to referencing table while there is NO corresponding value in referenced table
INSERT INTO referencing_table VALUES(1, 1);

-- test insert to referencing while there is corresponding value in referenced table
INSERT INTO referenced_table SELECT x, x%10 from generate_series(1,1000) as f(x);
INSERT INTO referencing_table SELECT x%10, x from generate_series(1,1000) as f(x);


-- test deletes
-- test delete from referenced table while there is corresponding value in referencing table
DELETE FROM referenced_table WHERE id > 3;

-- test delete from referenced table while there is NO corresponding value in referencing table
DELETE FROM referenced_table WHERE id = 6;

-- test cascading truncate
-- will fail for now
TRUNCATE referenced_table CASCADE;
SELECT count(*) FROM referencing_table;

-- drop table for next tests
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- self referencing foreign key on reference tables are not allowed
-- TODO try create_reference_table with already created foreign key.
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referencing_table');
-- self referencing foreign key
ALTER TABLE referenced_table ADD CONSTRAINT fkey_ref FOREIGN KEY (test_column) REFERENCES referenced_table(id);
-- foreign Keys from reference table to reference table are not allowed
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE CASCADE;

DROP TABLE referenced_table;
DROP TABLE referencing_table;

-- cascades on delete with different schemas
CREATE SCHEMA referenced_schema;
CREATE SCHEMA referencing_schema;
CREATE TABLE referenced_schema.referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_schema.referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_schema.referenced_table');
SELECT create_distributed_table('referencing_schema.referencing_table', 'id');
ALTER TABLE referencing_schema.referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_schema.referenced_table(id) ON DELETE CASCADE;

INSERT INTO referenced_schema.referenced_table SELECT x, x%10 from generate_series(1,1000) as f(x);
INSERT INTO referencing_schema.referencing_table SELECT x%10, x from generate_series(1,1000) as f(x);

DELETE FROM referenced_schema.referenced_table WHERE id > 800;
SELECT count(*) FROM referencing_schema.referencing_table;

DROP SCHEMA referenced_schema CASCADE;
DROP SCHEMA referencing_schema CASCADE;

-- on delete set update cascades properly
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE SET DEFAULT;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1,999) AS f(x);
INSERT INTO referenced_table VALUES (-1,-1);

DELETE FROM referenced_table WHERE test_column > 800;
SELECT count(*) FROM referencing_table WHERE ref_id = -1;

DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- foreign key as composite key 
CREATE TYPE composite AS (key1 int, key2 int);
SELECT run_command_on_workers($$CREATE TYPE fkey_reference_table.composite AS (key1 int, key2 int)$$);

CREATE TABLE referenced_table(test_column composite, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, referencing_composite composite);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (referencing_composite) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT (x, x+1)::composite FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, (x+1, x+2)::composite FROM generate_series(1,999) AS f(x);

DELETE FROM referenced_table WHERE (test_column).key1 > 900;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- Serial column type works properly when it is in the target of foreign constraint
CREATE TABLE referenced_table(test_column SERIAL PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table(test_column2) SELECT x%20 FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1,999) AS f(x);

DELETE FROM referenced_table WHERE test_column2 > 10;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- Serial column type works properly when it is in the source of foreign constraint
CREATE TABLE referenced_table(test_column int PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id SERIAL);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT x,x%20 FROM generate_series(1,1000) AS f(x);
-- Success for existing inserts
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,999) AS f(x);
-- Fails for non existing value inserts (serial is already incremented)
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,10) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- Serial column type works properly when it is in both source and target
CREATE TABLE referenced_table(test_column SERIAL PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id SERIAL);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table(test_column2) SELECT x%20 FROM generate_series(1,1000) AS f(x);
-- Success for existing values
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,1000) AS f(x);
-- Fails for non existing value inserts (serial is already incremented)
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,10) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- Volatile function on the column to ref
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE SET DEFAULT;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referencing_table SELECT x,(random()*1000)::int FROM generate_series(0,1000) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;


-- validate constraint
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE SET DEFAULT NOT VALID;

-- still applies the FK right after the creation
INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,999) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1000,1001) AS f(x);

-- currently fails
ALTER TABLE referencing_table VALIDATE CONSTRAINT fkey_ref;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- UPSERT on update set cascading
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON UPDATE CASCADE;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,999) AS f(x);

INSERT INTO referenced_table VALUES (1,2), (2,3), (3,4), (4,5)
ON CONFLICT (test_column)
DO UPDATE
  SET test_column = -1 * EXCLUDED.test_column;

SELECT * FROM referencing_table WHERE ref_id < 0 ORDER BY 1;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;


-- Chained references 
-- distributed table is referencing to two reference tables from one column
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref2 FOREIGN KEY (id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referenced_table2 SELECT x, x+1 FROM generate_series(500,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1000,1400) AS f(x);
-- should success
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(600,900) AS f(x);

SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table WHERE test_column < 700;
SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table2 WHERE test_column2 > 800;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;


-- distributed table is referencing to two reference tables from two columns
-- one is the distribution key one is not
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref2 FOREIGN KEY (ref_id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referenced_table2 SELECT x, x+1 FROM generate_series(500,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1000,1400) AS f(x);
-- should success
INSERT INTO referencing_table SELECT x, x+501 FROM generate_series(0,1000) AS f(x);

SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table WHERE test_column < 700;
SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table2 WHERE test_column2 > 800;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;


-- two distributed tables are referencing to one reference table and in the same the distributed table 2 is referencing to distributed table 1
-- distributed table 1 is referencing the distribution column
-- distributed table 2 is referencing non-distribution column
-- distributed table 2 has a reference to distributed table 1 on the distribution column
CREATE TABLE referenced_table(test_column int, test_column2 int UNIQUE, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int PRIMARY KEY, ref_id int);
CREATE TABLE referencing_table2(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
SELECT create_distributed_table('referencing_table2', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table2 ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column2) ON DELETE CASCADE;
ALTER TABLE referencing_table2 ADD CONSTRAINT fkey_ref_to_dist FOREIGN KEY (id) REFERENCES referencing_table(id) ON DELETE CASCADE;

SELECT * FROM num_of_foreign_keys;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
-- should fail
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(0,100) AS f(x);
-- should success
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(200,500) AS f(x);
-- should success
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(0,300) AS f(x);

DELETE FROM referenced_table WHERE test_column < 200;
SELECT count(*) FROM referencing_table;
SELECT count(*) FROM referencing_table2;
DELETE FROM referencing_table WHERE id > 200;
SELECT count(*) FROM referencing_table2;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
DROP TABLE referencing_table2 CASCADE;


-- on delete cascade with multi column FK in form of reference table <= distributed table <= distributed table
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column, test_column2));
CREATE TABLE referencing_table(id int, ref_id int, ref_id2 int, PRIMARY KEY(id, ref_id));
CREATE TABLE referencing_referencing_table(id int, ref_id int, FOREIGN KEY (id, ref_id) REFERENCES referencing_table(id, ref_id) ON DELETE CASCADE);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
SELECT create_distributed_table('referencing_referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id, ref_id2) REFERENCES referenced_table(test_column, test_column2) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1, x+2 FROM generate_series(1,999) AS f(x);
INSERT INTO referencing_referencing_table SELECT x, x+1 FROM generate_series(1,999) AS f(x);

DELETE FROM referenced_table WHERE test_column > 800;
SELECT max(ref_id) FROM referencing_referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
DROP TABLE referencing_referencing_table;

DROP SCHEMA fkey_reference_table CASCADE;
SET search_path TO DEFAULT;
