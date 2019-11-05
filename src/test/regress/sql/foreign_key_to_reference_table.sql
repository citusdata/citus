--
-- FOREIGN_KEY_TO_REFERENCE_TABLE
--

CREATE SCHEMA fkey_reference_table;
SET search_path TO 'fkey_reference_table';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 7000000;
SET citus.next_placement_id TO 7000000;

CREATE TYPE foreign_details AS (name text, relid text, refd_relid text);

CREATE VIEW table_fkeys_in_workers AS
SELECT
(json_populate_record(NULL::foreign_details,
  json_array_elements_text((run_command_on_workers( $$
    SELECT
      COALESCE(json_agg(row_to_json(d)), '[]'::json)
    FROM
      (
        SELECT
          distinct name,
          relid::regclass::text,
          refd_relid::regclass::text
        FROM
          table_fkey_cols
        WHERE
          "schema" = 'fkey_reference_table'
      )
      d $$ )).RESULT::json )::json )).* ;

CREATE TABLE referenced_table(id int UNIQUE, test_column int);
SELECT create_reference_table('referenced_table');

-- we still do not support update/delete operations through foreign constraints if the foreign key includes the distribution column
-- All should fail
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL);
SELECT create_distributed_table('referencing_table', 'ref_id');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT);
SELECT create_distributed_table('referencing_table', 'ref_id');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
DROP TABLE referencing_table;

BEGIN;
  CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL);
  SELECT create_distributed_table('referencing_table', 'ref_id');
ROLLBACK;

-- try with multiple columns including the distribution column
DROP TABLE referenced_table;
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id, test_column));
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE SET DEFAULT;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE SET DEFAULT);
SELECT create_distributed_table('referencing_table', 'ref_id');
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE CASCADE;
DROP TABLE referencing_table;

BEGIN;
  CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column) ON UPDATE CASCADE);
  SELECT create_distributed_table('referencing_table', 'ref_id');
ROLLBACK;

-- all of the above is supported if the foreign key does not include distribution column
DROP TABLE referenced_table;
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id));
SELECT create_reference_table('referenced_table');

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET NULL;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET NULL);
SELECT create_distributed_table('referencing_table', 'ref_id');
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

BEGIN;
  CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT);
  SELECT create_distributed_table('referencing_table', 'ref_id');
COMMIT;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE SET DEFAULT;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE CASCADE;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

-- check if we can add the foreign key while adding the column
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD COLUMN referencing int REFERENCES referenced_table(id) ON UPDATE CASCADE;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

-- foreign keys are only supported when the replication factor = 1
SET citus.shard_replication_factor TO 2;
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(id);
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

-- should fail when we add the column as well
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id');
ALTER TABLE referencing_table ADD COLUMN referencing_col int REFERENCES referenced_table(id) ON DELETE SET NULL;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;
SET citus.shard_replication_factor TO 1;

-- simple create_distributed_table should work in/out transactions on tables with foreign key to reference tables
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(id));
SELECT create_distributed_table('referencing_table', 'ref_id');
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

BEGIN;
  CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id));
  SELECT create_reference_table('referenced_table');
  CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(id));
  SELECT create_distributed_table('referencing_table', 'ref_id');
COMMIT;
SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

-- foreign keys are supported either in between distributed tables including the
-- distribution column or from distributed tables to reference tables.
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'append');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(id);
SELECT * FROM table_fkeys_in_workers WHERE name LIKE 'fkey_ref%' ORDER BY 1,2,3;
DROP TABLE referencing_table;

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'range');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(id);
SELECT * FROM table_fkeys_in_workers WHERE name LIKE 'fkey_ref%' ORDER BY 1,2,3;
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint with correct conditions
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(id);


-- test inserts
-- test insert to referencing table while there is NO corresponding value in referenced table
INSERT INTO referencing_table VALUES(1, 1);

-- test insert to referencing while there is corresponding value in referenced table
INSERT INTO referenced_table SELECT x, x from generate_series(1,1000) as f(x);
INSERT INTO referencing_table SELECT x, x from generate_series(1,500) as f(x);


-- test deletes
-- test delete from referenced table while there is corresponding value in referencing table
DELETE FROM referenced_table WHERE id > 3;

-- test delete from referenced table while there is NO corresponding value in referencing table
DELETE FROM referenced_table WHERE id = 501;

-- test cascading truncate
TRUNCATE referenced_table CASCADE;
SELECT count(*) FROM referencing_table;

-- drop table for next tests
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- self referencing foreign key on reference tables are allowed
-- TODO try create_reference_table with already created foreign key.
CREATE TABLE referenced_table(id int, test_column int, PRIMARY KEY(id));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referencing_table');
-- self referencing foreign key
ALTER TABLE referenced_table ADD CONSTRAINT fkey_ref FOREIGN KEY (test_column) REFERENCES referenced_table(id);
-- foreign Keys from reference table to reference table are allowed
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY(id) REFERENCES referenced_table(id) ON UPDATE CASCADE;

DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- cascades on delete with different schemas
CREATE SCHEMA referenced_schema;
CREATE SCHEMA referencing_schema;
CREATE TABLE referenced_schema.referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_schema.referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_schema.referenced_table');
SELECT create_distributed_table('referencing_schema.referencing_table', 'id');
ALTER TABLE referencing_schema.referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_schema.referenced_table(id) ON DELETE CASCADE;

INSERT INTO referenced_schema.referenced_table SELECT x, x from generate_series(1,1000) as f(x);
INSERT INTO referencing_schema.referencing_table SELECT x, x from generate_series(1,1000) as f(x);

DELETE FROM referenced_schema.referenced_table WHERE id > 800;
SELECT count(*) FROM referencing_schema.referencing_table;

SET client_min_messages TO ERROR;
DROP SCHEMA referenced_schema CASCADE;
DROP SCHEMA referencing_schema CASCADE;
RESET client_min_messages;

-- on delete set update cascades properly
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT 1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE SET DEFAULT;

INSERT INTO referenced_table SELECT x, x FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x FROM generate_series(1,1000) AS f(x);

DELETE FROM referenced_table WHERE test_column > 800;
SELECT count(*) FROM referencing_table WHERE ref_id = 1;

DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- foreign key as composite key
CREATE TYPE fkey_reference_table.composite AS (key1 int, key2 int);

CREATE TABLE referenced_table(test_column composite, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, referencing_composite composite);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (referencing_composite) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT (x+1, x+1)::composite FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, (x+1, x+1)::composite FROM generate_series(1,1000) AS f(x);

DELETE FROM referenced_table WHERE (test_column).key1 > 900;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- In the following test, we'll use a SERIAL column as the referenced column
-- in the foreign constraint. We'll first show that and insert on non-serial
-- column successfully inserts into the serial and referenced column.
-- Accordingly, the inserts into the referencing table which references to the
-- serial column will be successful.
CREATE TABLE referenced_table(test_column SERIAL PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table(test_column2) SELECT x FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x FROM generate_series(1,1000) AS f(x);

DELETE FROM referenced_table WHERE test_column2 > 10;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- In the following test, we'll use a SERIAL column as the referencing column
-- in the foreign constraint. We'll first show that the values that exist
-- in the referenced tables are successfully generated by the serial column
-- and inserted to the distributed table. However,  if the values that are generated
-- by serial column do not exist on the referenced table, the query fails.
CREATE TABLE referenced_table(test_column int PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id SERIAL);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table SELECT x,x FROM generate_series(1,1000) AS f(x);
-- Success for existing inserts
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,1000) AS f(x);
-- Fails for non existing value inserts (serial is already incremented)
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,10) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- In the following test, we'll use a SERIAL column as the referencing column
-- and referenced columns in a foreign constraint. We'll first show that the
-- the inserts into referenced column will successfully generate and insert
-- data into serial column. Then, we will be successfully insert the same amount
-- of data into referencing table. However,  if the values that are generated
-- by serial column do not exist on the referenced table, the query fails.
CREATE TABLE referenced_table(test_column SERIAL PRIMARY KEY, test_column2 int);
CREATE TABLE referencing_table(id int, ref_id SERIAL);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;

INSERT INTO referenced_table(test_column2) SELECT x FROM generate_series(1,1000) AS f(x);
-- Success for existing values
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,1000) AS f(x);
-- Fails for non existing value inserts (serial is already incremented)
INSERT INTO referencing_table(id) SELECT x FROM generate_series(1,10) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
-- In the following test, we use a volatile function in the referencing
-- column in a foreign constraint. We show that if the data exists in the
-- referenced table, we can successfully use volatile functions with
-- foreign constraints.
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON DELETE SET DEFAULT;

INSERT INTO referenced_table SELECT x, x FROM generate_series(0,1000) AS f(x);
INSERT INTO referencing_table SELECT x,(random()*1000)::int FROM generate_series(0,1000) AS f(x);

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- In the following tests, we create a foreign constraint with
-- ON UPDATE CASCADE and see if it works properly with cascading upsert
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON UPDATE CASCADE;

INSERT INTO referenced_table SELECT x, x FROM generate_series(0,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x FROM generate_series(0,1000) AS f(x);

INSERT INTO referenced_table VALUES (1,2), (2,3), (3,4), (4,5)
ON CONFLICT (test_column)
DO UPDATE
  SET test_column = -1 * EXCLUDED.test_column;

SELECT * FROM referencing_table WHERE ref_id < 0 ORDER BY 1;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- create_distributed_table should fail for tables with data if fkey exists to reference table
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int, ref_id int DEFAULT -1, FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column) ON UPDATE CASCADE);
INSERT INTO referenced_table VALUES (1,1), (2,2), (3,3);
INSERT INTO referencing_table VALUES (1,1), (2,2), (3,3);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');

BEGIN;
  SELECT create_distributed_table('referencing_table', 'id');
COMMIT;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;

-- Chained references
-- In the following test, we create foreign keys from one column in a distributed
-- table to two reference tables. We expect to see that even if a data exist in
-- one reference table, it is not going to be inserted in to referencing table
-- because of lack of the key in the other table. Data can only be inserted into
-- referencing table if it exists in both referenced tables.
-- Additionally, delete or update in one referenced table should cascade properly.
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table ADD CONSTRAINT foreign_key_2 FOREIGN KEY (id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;

SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referenced_table2 SELECT x, x+1 FROM generate_series(500,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1000,1400) AS f(x);
-- should succeed
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(600,900) AS f(x);

SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table WHERE test_column < 700;
SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table2 WHERE test_column2 > 800;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;

-- check if the above fkeys are created with create_distributed_table
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE, FOREIGN KEY (id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';

\set VERBOSITY terse
DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;

-- In the following test, we create foreign keys from two columns in a distributed
-- table to two reference tables separately. We expect to see that even if a data
-- exist in one reference table for one column, it is not going to be inserted in
-- to referencing table because the other constraint doesn't hold. Data can only
-- be inserted into referencing table if both columns exist in respective columns
-- in referenced tables.
-- Additionally, delete or update in one referenced table should cascade properly.
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_reference_table('referenced_table2');
SELECT create_distributed_table('referencing_table', 'id');

BEGIN;
  ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
  ALTER TABLE referencing_table ADD CONSTRAINT foreign_key_2 FOREIGN KEY (ref_id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;
COMMIT;

SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
INSERT INTO referenced_table2 SELECT x, x+1 FROM generate_series(500,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,1500) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(1000,1400) AS f(x);
-- should succeed
INSERT INTO referencing_table SELECT x, x+501 FROM generate_series(0,1000) AS f(x);

SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table WHERE test_column < 700;
SELECT count(*) FROM referencing_table;
DELETE FROM referenced_table2 WHERE test_column2 > 800;
SELECT count(*) FROM referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;

-- check if the above fkeys are created when create_distributed_table is used for 1 foreign key and alter table for the other
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column));
CREATE TABLE referenced_table2(test_column int, test_column2 int, PRIMARY KEY(test_column2));
CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE);

BEGIN;
  SELECT create_reference_table('referenced_table');
  SELECT create_reference_table('referenced_table2');
  SELECT create_distributed_table('referencing_table', 'id');
  ALTER TABLE referencing_table ADD CONSTRAINT foreign_key_2 FOREIGN KEY (ref_id) REFERENCES referenced_table2(test_column2) ON DELETE CASCADE;
COMMIT;

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';

DROP TABLE referenced_table CASCADE;
DROP TABLE referenced_table2 CASCADE;
DROP TABLE referencing_table CASCADE;
\set VERBOSITY default


-- two distributed tables are referencing to one reference table and
-- in the same time the distributed table 2 is referencing to
-- distributed table 1. Thus, we have a triangular
-- distributed table 1 has a foreign key from the distribution column to reference table
-- distributed table 2 has a foreign key from a non-distribution column to reference table
-- distributed table 2 has a foreign key to distributed table 1 on the distribution column
-- We show that inserts into distributed table 2 will fail if the data does not exist in distributed table 1
-- Delete from reference table cascades to both of the distributed tables properly
CREATE TABLE referenced_table(test_column int, test_column2 int UNIQUE, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int PRIMARY KEY, ref_id int);
CREATE TABLE referencing_table2(id int, ref_id int);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
SELECT create_distributed_table('referencing_table2', 'id');
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE;
ALTER TABLE referencing_table2 ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column2) ON DELETE CASCADE;
ALTER TABLE referencing_table2 ADD CONSTRAINT fkey_ref_to_dist FOREIGN KEY (id) REFERENCES referencing_table(id) ON DELETE CASCADE;
COMMIT;

SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%' ORDER BY 1,2,3;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(0,1000) AS f(x);
-- should fail
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(0,100) AS f(x);
-- should succeed
INSERT INTO referencing_table SELECT x, x+1 FROM generate_series(0,400) AS f(x);
-- should fail
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(200,500) AS f(x);
-- should succeed
INSERT INTO referencing_table2 SELECT x, x+1 FROM generate_series(0,300) AS f(x);

DELETE FROM referenced_table WHERE test_column < 200;
SELECT count(*) FROM referencing_table;
SELECT count(*) FROM referencing_table2;
DELETE FROM referencing_table WHERE id > 200;
SELECT count(*) FROM referencing_table2;

\set VERBOSITY terse
DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
DROP TABLE referencing_table2 CASCADE;
\set VERBOSITY default

-- Check if the above fkeys are created with create_distributed_table
CREATE TABLE referenced_table(test_column int, test_column2 int UNIQUE, PRIMARY KEY(test_column));
CREATE TABLE referencing_table(id int PRIMARY KEY, ref_id int, FOREIGN KEY (id) REFERENCES referenced_table(test_column) ON DELETE CASCADE);
CREATE TABLE referencing_table2(id int, ref_id int, FOREIGN KEY (ref_id) REFERENCES referenced_table(test_column2) ON DELETE CASCADE, FOREIGN KEY (id) REFERENCES referencing_table(id) ON DELETE CASCADE);
SELECT create_reference_table('referenced_table');
BEGIN;
  SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
  SELECT create_distributed_table('referencing_table', 'id');
  SELECT create_distributed_table('referencing_table2', 'id');
COMMIT;

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';

\set VERBOSITY terse
DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
DROP TABLE referencing_table2 CASCADE;
\set VERBOSITY default

-- In this test we have a chained relationship in form of
-- distributed table (referencing_referencing_table) has a foreign key with two columns
-- to another distributed table (referencing_table)
-- referencing_table has another foreign key with 2 columns to referenced_table.
-- We will show that a cascading delete on referenced_table reaches to referencing_referencing_table.
CREATE TABLE referenced_table(test_column int, test_column2 int, PRIMARY KEY(test_column, test_column2));
CREATE TABLE referencing_table(id int, ref_id int, ref_id2 int, PRIMARY KEY(id, ref_id));
CREATE TABLE referencing_referencing_table(id int, ref_id int, FOREIGN KEY (id, ref_id) REFERENCES referencing_table(id, ref_id) ON DELETE CASCADE);
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');
SELECT create_distributed_table('referencing_referencing_table', 'id');
ALTER TABLE referencing_table ADD CONSTRAINT fkey_ref FOREIGN KEY (ref_id, ref_id2) REFERENCES referenced_table(test_column, test_column2) ON DELETE CASCADE;

SELECT * FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.referencing%' ORDER BY 1,2,3;

INSERT INTO referenced_table SELECT x, x+1 FROM generate_series(1,1000) AS f(x);
INSERT INTO referencing_table SELECT x, x+1, x+2 FROM generate_series(1,999) AS f(x);
INSERT INTO referencing_referencing_table SELECT x, x+1 FROM generate_series(1,999) AS f(x);

DELETE FROM referenced_table WHERE test_column > 800;
SELECT max(ref_id) FROM referencing_referencing_table;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table CASCADE;
DROP TABLE referencing_referencing_table;

-- test if create_distributed_table works in transactions with some edge cases
-- the following checks if create_distributed_table works on foreign keys when
-- one of them is a self-referencing table of multiple distributed tables
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY);
  SELECT create_reference_table('test_table_1');

  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(id) REFERENCES test_table_1(id));
  SELECT create_distributed_table('test_table_2', 'id');

  CREATE TABLE test_table_3(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id), FOREIGN KEY(id) REFERENCES test_table_2(id));
  SELECT create_distributed_table('test_table_3', 'id');

  DROP TABLE test_table_1 CASCADE;
ROLLBACK;

-- create_reference_table, create_distributed_table and ALTER TABLE in the same transaction
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY);
  SELECT create_reference_table('test_table_1');

  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int);
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_2 ADD CONSTRAINT c_check FOREIGN KEY (value_1) REFERENCES test_table_1(id);

  DROP TABLE test_table_1, test_table_2;
COMMIT;

-- the order of create_reference_table and create_distributed_table is changed
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY, value_1 int);
  SELECT create_distributed_table('test_table_1', 'id');

  CREATE TABLE test_table_2(id int PRIMARY KEY);
  SELECT create_reference_table('test_table_2');

  ALTER TABLE test_table_1 ADD CONSTRAINT c_check FOREIGN KEY (value_1) REFERENCES test_table_2(id);

  DROP TABLE test_table_2 CASCADE;
ROLLBACK;

-- make sure that we fail if we need parallel data load
BEGIN;

  CREATE TABLE test_table_1(id int PRIMARY KEY);
  INSERT INTO test_table_1 SELECT i FROM generate_series(0,100) i;

  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));
  INSERT INTO test_table_2 SELECT i, i FROM generate_series(0,100) i;

  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');
  DROP TABLE test_table_2, test_table_1;
COMMIT;

-- make sure that other DDLs/DMLs also work fine
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY);
  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  CREATE INDEX i1 ON test_table_1(id);
  ALTER TABLE test_table_2 ADD CONSTRAINT check_val CHECK (id > 0);

  DROP TABLE test_table_2, test_table_1;
COMMIT;

-- The following tests check if the DDLs affecting foreign keys work as expected
-- check if we can drop the foreign constraint
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';

ALTER TABLE test_table_2 DROP CONSTRAINT test_table_2_value_1_fkey;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';

DROP TABLE test_table_1, test_table_2;

-- check if we can drop the foreign constraint in a transaction right after ADD CONSTRAINT
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY);
  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int);

  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_2 ADD CONSTRAINT foreign_key FOREIGN KEY(value_1) REFERENCES test_table_1(id);
  ALTER TABLE test_table_2 DROP CONSTRAINT test_table_2_value_1_fkey;
COMMIT;

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the primary key which cascades to the foreign key
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

ALTER TABLE test_table_1 DROP CONSTRAINT test_table_1_pkey CASCADE;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the primary key which cascades to the foreign key in a transaction block
BEGIN;
  CREATE TABLE test_table_1(id int PRIMARY KEY);
  CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_1 DROP CONSTRAINT test_table_1_pkey CASCADE;
COMMIT;

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the column which foreign key is referencing from
CREATE TABLE test_table_1(id int PRIMARY KEY, id2 int);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

ALTER TABLE test_table_2 DROP COLUMN value_1;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the column which foreign key is referencing from in a transaction block
CREATE TABLE test_table_1(id int PRIMARY KEY, id2 int);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

BEGIN;
  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_2 DROP COLUMN value_1;
COMMIT;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the column which foreign key is referencing to
CREATE TABLE test_table_1(id int PRIMARY KEY, id2 int);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

ALTER TABLE test_table_1 DROP COLUMN id CASCADE;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can drop the column which foreign key is referencing from in a transaction block
CREATE TABLE test_table_1(id int PRIMARY KEY, id2 int);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

BEGIN;
  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_1 DROP COLUMN id CASCADE;
COMMIT;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can alter the column type which foreign key is referencing to
CREATE TABLE test_table_1(id int PRIMARY KEY, id2 int);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');
INSERT INTO test_table_1 VALUES (1,1), (2,2), (3,3);
INSERT INTO test_table_2 VALUES (1,1), (2,2), (3,3);

-- should succeed
ALTER TABLE test_table_2 ALTER COLUMN value_1 SET DATA TYPE bigint;
ALTER TABLE test_table_1 ALTER COLUMN id SET DATA TYPE bigint;

INSERT INTO test_table_1 VALUES (2147483648,4);
INSERT INTO test_table_2 VALUES (4,2147483648);
-- should fail since there is a bigint out of integer range > (2^32 - 1)
ALTER TABLE test_table_2 ALTER COLUMN value_1 SET DATA TYPE int;
SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1 CASCADE;
DROP TABLE test_table_2;

-- check if we can alter the column type and drop it which foreign key is referencing to in a transaction block
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

BEGIN;
  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  ALTER TABLE test_table_2 ALTER COLUMN value_1 SET DATA TYPE bigint;
  ALTER TABLE test_table_1 DROP COLUMN id CASCADE;
COMMIT;

SELECT count(*) FROM table_fkeys_in_workers WHERE relid LIKE 'fkey_reference_table.%' AND refd_relid LIKE 'fkey_reference_table.%';
DROP TABLE test_table_1, test_table_2;

-- check if we can TRUNCATE the referenced table
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));
SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

INSERT INTO test_table_1 VALUES (1),(2),(3);
INSERT INTO test_table_2 VALUES (1,1),(2,2),(3,3);
TRUNCATE test_table_1 CASCADE;

SELECT * FROM test_table_2;
DROP TABLE test_table_1, test_table_2;

-- check if we can TRUNCATE the referenced table in a transaction
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));
SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

INSERT INTO test_table_1 VALUES (1),(2),(3);
INSERT INTO test_table_2 VALUES (1,1),(2,2),(3,3);

BEGIN;
  TRUNCATE test_table_1 CASCADE;
COMMIT;
SELECT * FROM test_table_2;
DROP TABLE test_table_1, test_table_2;

-- check if we can TRUNCATE the referenced table in a transaction after inserts
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));

BEGIN;
  SELECT create_reference_table('test_table_1');
  SELECT create_distributed_table('test_table_2', 'id');

  INSERT INTO test_table_1 VALUES (1),(2),(3);
  INSERT INTO test_table_2 VALUES (1,1),(2,2),(3,3);
  TRUNCATE test_table_1 CASCADE;
COMMIT;

SELECT * FROM test_table_2;
DROP TABLE test_table_1, test_table_2;

-- check if we can TRUNCATE the referencing table
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));
SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

INSERT INTO test_table_1 VALUES (1),(2),(3);
INSERT INTO test_table_2 VALUES (1,1),(2,2),(3,3);
TRUNCATE test_table_2 CASCADE;

SELECT * FROM test_table_2;
SELECT * FROM test_table_1;
DROP TABLE test_table_1, test_table_2;

-- check if we can TRUNCATE the referencing table in a transaction
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int, FOREIGN KEY(value_1) REFERENCES test_table_1(id));
SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');

INSERT INTO test_table_1 VALUES (1),(2),(3);
INSERT INTO test_table_2 VALUES (1,1),(2,2),(3,3);
BEGIN;
  TRUNCATE test_table_2 CASCADE;
COMMIT;

SELECT * FROM test_table_2;
SELECT * FROM test_table_1;
DROP TABLE test_table_1, test_table_2;

-- check if we successfuly set multi_shard_modify_mode to sequential after sequentially running DDLs
-- in transaction since the upcoming DDLs need to run sequentially.
CREATE TABLE test_table_1(id int PRIMARY KEY);
CREATE TABLE test_table_2(id int PRIMARY KEY, value_1 int);
CREATE TABLE test_table_3(id int PRIMARY KEY, value_1 int);
SELECT create_reference_table('test_table_1');
SELECT create_distributed_table('test_table_2', 'id');
SELECT create_distributed_table('test_table_3', 'id');
BEGIN;
  ALTER TABLE test_table_2 ADD CONSTRAINT fkey FOREIGN KEY (value_1) REFERENCES test_table_1(id);
  ALTER TABLE test_table_3 ADD COLUMN test_column int;
  ALTER TABLE test_table_1 DROP COLUMN id CASCADE;
  ALTER TABLE test_table_1 ADD COLUMN id int;
COMMIT;
DROP TABLE test_table_1, test_table_2, test_table_3;
-- NOTE: Postgres does not support foreign keys on partitioned tables currently.
-- However, we can create foreign keys to/from the partitions themselves.
-- The following tests chech if we create the foreign constraints in partitions properly.
CREATE TABLE referenced_table(id int PRIMARY KEY, test_column int);
CREATE TABLE referencing_table(id int, value_1 int) PARTITION BY RANGE (value_1);
CREATE TABLE referencing_table_0 PARTITION OF referencing_table FOR VALUES FROM (0) TO (2);
CREATE TABLE referencing_table_2 PARTITION OF referencing_table FOR VALUES FROM (2) TO (4);
CREATE TABLE referencing_table_4 PARTITION OF referencing_table FOR VALUES FROM (4) TO (6);

-- partitioned tables are not supported as reference tables
select create_reference_table('referencing_table');

-- partitioned tables are supported as hash distributed table
SELECT create_reference_table('referenced_table');
SELECT create_distributed_table('referencing_table', 'id');

-- add foreign constraints in between partitions
ALTER TABLE referencing_table_0 ADD CONSTRAINT pkey PRIMARY KEY (id);
ALTER TABLE referencing_table_4 ADD CONSTRAINT fkey FOREIGN KEY (id) REFERENCES referencing_table_0;
-- add foreign constraint from a partition to reference table
ALTER TABLE referencing_table_4 ADD CONSTRAINT fkey_to_ref FOREIGN KEY (value_1) REFERENCES referenced_table;
-- should fail since the data will flow to partitioning_test_4 and it has a foreign constraint to partitioning_test_0 on id column
INSERT INTO referencing_table VALUES (0, 5);
-- should succeed on partitioning_test_0
INSERT INTO referencing_table VALUES (0, 1);
SELECT * FROM referencing_table;
-- should fail since partitioning_test_4 has foreign constraint to referenced_table on value_1 column
INSERT INTO referencing_table VALUES (0, 5);
INSERT INTO referenced_table VALUES(5,5);
-- should succeed since both of the foreign constraints are positive
INSERT INTO referencing_table VALUES (0, 5);

-- TRUNCATE should work in any way
TRUNCATE referencing_table, referenced_table;
TRUNCATE referenced_table, referencing_table;

BEGIN;
  TRUNCATE referencing_table, referenced_table;
  ALTER TABLE referencing_table ADD COLUMN x INT;
  SELECT * FROM referencing_table;
ROLLBACK;

BEGIN;
  TRUNCATE referenced_table, referencing_table;
  ALTER TABLE referencing_table ADD COLUMN x INT;
  SELECT * FROM referencing_table;
ROLLBACK;

DROP TABLE referenced_table CASCADE;
DROP TABLE referencing_table;

SET client_min_messages TO ERROR;
DROP SCHEMA fkey_reference_table CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
