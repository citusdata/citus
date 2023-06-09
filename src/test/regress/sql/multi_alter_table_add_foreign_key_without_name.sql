--
-- MULTI_ALTER_TABLE_ADD_FOREIGN_KEY_WITHOUT_NAME
--
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1770000;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 1770000;

SET citus.shard_count TO 4;

CREATE SCHEMA at_add_fk;
SET SEARCH_PATH = at_add_fk;
SET citus.shard_replication_factor TO 1;

-- create tables
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
SELECT create_distributed_table('referenced_table', 'id', 'hash');

-- self referencing table with replication factor > 1
SET citus.shard_replication_factor TO 2;
CREATE TABLE self_referencing_table(id int, ref_id int, PRIMARY KEY (id, ref_id));
SELECT create_distributed_table('self_referencing_table', 'id', 'hash');
ALTER TABLE self_referencing_table ADD FOREIGN KEY(id,ref_id) REFERENCES self_referencing_table(id, ref_id);
DROP TABLE self_referencing_table;

-- test foreign constraint creation on NOT co-located tables
SET citus.shard_replication_factor TO 1;
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id);
DROP TABLE referencing_table;

-- test foreign constraint creation on non-partition columns
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
ALTER TABLE referencing_table ADD FOREIGN KEY(id) REFERENCES referenced_table(id);
DROP TABLE referencing_table;

-- test foreign constraint creation while column list are in incorrect order
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
ALTER TABLE referencing_table ADD FOREIGN KEY(id, ref_id) REFERENCES referenced_table(id, test_column);
DROP TABLE referencing_table;

-- test foreign constraint with replication factor > 1
SET citus.shard_replication_factor TO 2;
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id);
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint creation on append and range distributed tables
-- foreign keys are supported either in between distributed tables including the
-- distribution column or from distributed tables to reference tables.
SET citus.shard_replication_factor TO 1;
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
SELECT create_distributed_table('referenced_table', 'id', 'hash');

CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'id', 'append');
ALTER TABLE referencing_table ADD FOREIGN KEY (id) REFERENCES referenced_table(id);
DROP TABLE referencing_table;
DROP TABLE referenced_table;

CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
SELECT create_distributed_table('referenced_table', 'id', 'range');
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referencing_table', 'id', 'range');
ALTER TABLE referencing_table ADD FOREIGN KEY (id) REFERENCES referenced_table(id);
DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint creation is supported when coordinator is in metadata
CREATE TABLE referenced_local_table(id int PRIMARY KEY, other_column int);
CREATE TABLE reference_table(id int, referencing_column int);
SELECT create_reference_table('reference_table');

ALTER TABLE reference_table ADD FOREIGN KEY (referencing_column) REFERENCES referenced_local_table(id);
DROP TABLE referenced_local_table;
DROP TABLE reference_table CASCADE;

-- test foreign constraint with correct conditions
CREATE TABLE referenced_table(id int PRIMARY KEY, test_column int);
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table' ORDER BY con.conname ASC;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;
ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

-- Test "ADD FOREIGN KEY (...) REFERENCING pk_table" format
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table;

SELECT con.conname
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table' ORDER BY con.conname ASC;

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;
ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

DROP TABLE referencing_table;
DROP TABLE referenced_table;

-- test foreign constraint options
-- test ON DELETE CASCADE
SET citus.shard_replication_factor TO 1;
CREATE TABLE referenced_table(id int UNIQUE, test_column int, PRIMARY KEY(id, test_column));
CREATE TABLE referencing_table(id int, ref_id int);
SELECT create_distributed_table('referenced_table', 'id', 'hash');
SELECT create_distributed_table('referencing_table', 'ref_id', 'hash');

ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE CASCADE;

SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

-- test NOT VALID
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) NOT VALID;
SELECT  con.conname, con.convalidated
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';

\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.convalidated
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;
ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

-- test ON DELETE NO ACTION + DEFERABLE + INITIALLY DEFERRED
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED;

SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;
ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

-- test ON DELETE RESTRICT

ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE RESTRICT;

SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_fkey;

-- test ON UPDATE NO ACTION + DEFERABLE + INITIALLY DEFERRED
ALTER TABLE  referencing_table ADD FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED;
SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_id_fkey;

-- test ON UPDATE RESTRICT
ALTER TABLE  referencing_table ADD FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) ON UPDATE RESTRICT;
SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_id_fkey;

-- test MATCH SIMPLE
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH SIMPLE;

SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_id_fkey;

-- test MATCH FULL
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id, id) REFERENCES referenced_table(id, test_column) MATCH FULL;
SELECT  con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'referencing_table';


\c - - :public_worker_1_host :worker_1_port
SELECT con.conname, con.confupdtype, con.confdeltype, con.confmatchtype
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'referencing_table%' ORDER BY con.conname ASC;

\c - - :master_host :master_port
SET SEARCH_PATH = at_add_fk;

ALTER TABLE  referencing_table DROP CONSTRAINT referencing_table_ref_id_id_fkey;

-- verify that we skip foreign key validation when citus.skip_constraint_validation is set to ON
-- not skipping validation would result in a distributed query, which emits debug messages
BEGIN;
SET LOCAL citus.skip_constraint_validation TO on;
SET LOCAL client_min_messages TO DEBUG1;
ALTER TABLE referencing_table ADD FOREIGN KEY (ref_id) REFERENCES referenced_table (id);
ABORT;

-- test foreign constraint creation with not supported parameters
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET NULL;
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT;
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET NULL;
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE SET DEFAULT;
ALTER TABLE referencing_table ADD FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON UPDATE CASCADE;


-- test ADD FOREIGN KEY from distributed to reference table.
SET citus.shard_replication_factor = 1;
CREATE TABLE dist_table(id int, referencing_column int);
SELECT create_distributed_table('dist_table', 'referencing_column');

CREATE TABLE reference_table(id int PRIMARY KEY, another_column int);
SELECT create_reference_table('reference_table');

ALTER TABLE dist_table ADD FOREIGN KEY(referencing_column) REFERENCES reference_table(id);

SELECT  con.conname
  FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'dist_table';

ALTER TABLE dist_table DROP CONSTRAINT dist_table_referencing_column_fkey;

BEGIN;
	SELECT count(*) FROM  dist_table;
	ALTER TABLE  dist_table ADD FOREIGN KEY(referencing_column) REFERENCES reference_table(id);
	ROLLBACK;

-- try inside a sequential block
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM  dist_table;
	ALTER TABLE  dist_table ADD FOREIGN KEY(referencing_column) REFERENCES reference_table(id);
	ROLLBACK;

DROP TABLE dist_table CASCADE;
DROP TABLE reference_table CASCADE;

-- test ADD FOREIGN KEY from citus local to reference table
CREATE TABLE citus_local_table(l1 int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

CREATE TABLE reference_table(r1 int primary key); SELECT create_reference_table('reference_table');
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- test ADD FOREIGN KEY from citus local to reference table with supported options
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE CASCADE;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE SET NULL;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE SET DEFAULT;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE NO ACTION;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE RESTRICT;

ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE CASCADE;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE SET NULL;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE SET DEFAULT;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE NO ACTION;
ALTER TABLE citus_local_table ADD FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE RESTRICT;

DROP TABLE citus_local_table CASCADE;

RESET SEARCH_PATH;
RESET client_min_messages;
DROP SCHEMA at_add_fk CASCADE;

-- test ADD FOREIGN KEY when REFERENCED table is in another schema.
CREATE SCHEMA schema_1;

CREATE TABLE schema_1.referenced_table(a int PRIMARY KEY, b int);
SELECT create_reference_table('schema_1.referenced_table');

CREATE SCHEMA schema_2;

CREATE TABLE schema_2.referencing_table (a int PRIMARY KEY, b int, c text);
ALTER TABLE schema_2.referencing_table ADD FOREIGN KEY (b) REFERENCES schema_1.referenced_table(a);

DROP SCHEMA schema_1, schema_2 CASCADE;
