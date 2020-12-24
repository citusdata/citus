SET citus.next_shard_id TO 3000000;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA fkey_graph;
SET search_path TO 'fkey_graph';

CREATE FUNCTION get_referencing_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referencing_relation_id_list$$;

CREATE FUNCTION get_referenced_relation_id_list(Oid)
    RETURNS SETOF Oid
    LANGUAGE C STABLE STRICT
    AS 'citus', $$get_referenced_relation_id_list$$;

-- Simple case with distributed tables
CREATE TABLE dtt1(id int PRIMARY KEY);
SELECT create_distributed_table('dtt1','id');

CREATE TABLE dtt2(id int PRIMARY KEY REFERENCES dtt1(id));
SELECT create_distributed_table('dtt2','id');

CREATE TABLE dtt3(id int PRIMARY KEY REFERENCES dtt2(id));
SELECT create_distributed_table('dtt3','id');

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;


CREATE TABLE dtt4(id int PRIMARY KEY);
SELECT create_distributed_table('dtt4', 'id');

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

ALTER TABLE dtt4 ADD CONSTRAINT dtt4_fkey FOREIGN KEY (id) REFERENCES dtt3(id);

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt1'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt2'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;

ALTER TABLE dtt4 DROP CONSTRAINT dtt4_fkey;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt3'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt3'::regclass) ORDER BY 1;

SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('dtt4'::regclass) ORDER BY 1;
SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('dtt4'::regclass) ORDER BY 1;

-- some tests within transction blocks to make sure that
-- cache invalidation works fine
CREATE TABLE test_1 (id int UNIQUE);
CREATE TABLE test_2 (id int UNIQUE);
CREATE TABLE test_3 (id int UNIQUE);
CREATE TABLE test_4 (id int UNIQUE);
CREATE TABLE test_5 (id int UNIQUE);

SELECT create_distributed_Table('test_1', 'id');
SELECT create_distributed_Table('test_2', 'id');
SELECT create_distributed_Table('test_3', 'id');
SELECT create_distributed_Table('test_4', 'id');
SELECT create_distributed_Table('test_5', 'id');

CREATE VIEW referential_integrity_summary AS
    WITH RECURSIVE referential_integrity_summary(n, table_name, referencing_relations, referenced_relations) AS
    (
        SELECT 0,'0','{}'::regclass[],'{}'::regclass[]
      UNION ALL
        SELECT
          n + 1,
          'test_' || n + 1|| '' as table_name,
          (SELECT  array_agg(get_referencing_relation_id_list::regclass ORDER BY 1) FROM get_referencing_relation_id_list(('test_' || (n +1) ) ::regclass)) as referencing_relations,
          (SELECT  array_agg(get_referenced_relation_id_list::regclass ORDER BY 1) FROM get_referenced_relation_id_list(('test_' || (n +1) ) ::regclass)) as referenced_by_relations
        FROM referential_integrity_summary, pg_class
        WHERE
         pg_class.relname = ('test_' || (n +1))
        AND n < 5
    )
    SELECT * FROM referential_integrity_summary WHERE n != 0 ORDER BY 1;

-- make sure that invalidation through ALTER TABLE works fine
BEGIN;
    ALTER TABLE test_2 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_1(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_3 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_2(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_4 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_3(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_5 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_4(id);
    SELECT * FROM referential_integrity_summary;
ROLLBACK;

-- similar test, but slightly different order of creating foreign keys
BEGIN;
    ALTER TABLE test_2 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_1(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_4 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_3(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_5 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_4(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_3 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_2(id);
    SELECT * FROM referential_integrity_summary;
ROLLBACK;

-- make sure that DROP CONSTRAINT works invalidates the cache correctly
BEGIN;
    ALTER TABLE test_2 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_1(id);
    ALTER TABLE test_3 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_2(id);
    ALTER TABLE test_4 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_3(id);
    ALTER TABLE test_5 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_4(id);
    SELECT * FROM referential_integrity_summary;
    ALTER TABLE test_3 DROP CONSTRAINT fkey_1;
    SELECT * FROM referential_integrity_summary;
ROLLBACK;

-- make sure that CREATE TABLE invalidates the cache correctly
DROP TABLE test_1, test_2, test_3, test_4, test_5 CASCADE;

BEGIN;
    CREATE TABLE test_1 (id int UNIQUE);
    SELECT create_distributed_Table('test_1', 'id');
    CREATE TABLE test_2 (id int UNIQUE, FOREIGN KEY(id) REFERENCES test_1(id));
    SELECT create_distributed_Table('test_2', 'id');
    SELECT * FROM referential_integrity_summary;
    CREATE TABLE test_3 (id int UNIQUE, FOREIGN KEY(id) REFERENCES test_2(id));
    SELECT create_distributed_Table('test_3', 'id');
    SELECT * FROM referential_integrity_summary;
    CREATE TABLE test_4 (id int UNIQUE, FOREIGN KEY(id) REFERENCES test_3(id));
    SELECT create_distributed_Table('test_4', 'id');
    SELECT * FROM referential_integrity_summary;
    CREATE TABLE test_5 (id int UNIQUE, FOREIGN KEY(id) REFERENCES test_4(id));
    SELECT create_distributed_Table('test_5', 'id');
    SELECT * FROM referential_integrity_summary;
COMMIT;

-- DROP TABLE works expected
-- re-create the constraints
BEGIN;
    ALTER TABLE test_2 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_1(id);
    ALTER TABLE test_3 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_2(id);
    ALTER TABLE test_4 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_3(id);
    ALTER TABLE test_5 ADD CONSTRAINT fkey_1 FOREIGN KEY(id) REFERENCES test_4(id);
    SELECT * FROM referential_integrity_summary;

    DROP TABLE test_3 CASCADE;
    SELECT * FROM referential_integrity_summary;
ROLLBACK;

-- Test schemas
BEGIN;
    CREATE SCHEMA fkey_intermediate_schema_1;
    CREATE SCHEMA fkey_intermediate_schema_2;
    SET search_path TO fkey_graph, fkey_intermediate_schema_1, fkey_intermediate_schema_2;

    CREATE TABLE fkey_intermediate_schema_1.test_6(id int PRIMARY KEY);
    SELECT create_distributed_table('fkey_intermediate_schema_1.test_6', 'id');

    CREATE TABLE fkey_intermediate_schema_2.test_7(id int PRIMARY KEY REFERENCES fkey_intermediate_schema_1.test_6(id));
    SELECT create_distributed_table('fkey_intermediate_schema_2.test_7','id');

    CREATE TABLE fkey_intermediate_schema_1.test_8(id int PRIMARY KEY REFERENCES fkey_intermediate_schema_2.test_7(id));
    SELECT create_distributed_table('fkey_intermediate_schema_1.test_8', 'id');

    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_7'::regclass) ORDER BY 1;
    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_8'::regclass) ORDER BY 1;

    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_7'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_8'::regclass) ORDER BY 1;

    DROP SCHEMA fkey_intermediate_schema_2 CASCADE;

    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_8'::regclass) ORDER BY 1;

    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_8'::regclass) ORDER BY 1;
ROLLBACK;

BEGIN;
    CREATE SCHEMA fkey_intermediate_schema_1;
    CREATE SCHEMA fkey_intermediate_schema_2;
    SET search_path TO fkey_graph, fkey_intermediate_schema_1, fkey_intermediate_schema_2;

    CREATE TABLE fkey_intermediate_schema_1.test_6(id int PRIMARY KEY);
    SELECT create_distributed_table('fkey_intermediate_schema_1.test_6', 'id');

    CREATE TABLE fkey_intermediate_schema_2.test_7(id int PRIMARY KEY REFERENCES fkey_intermediate_schema_1.test_6(id));
    SELECT create_distributed_table('fkey_intermediate_schema_2.test_7','id');

    CREATE TABLE fkey_intermediate_schema_1.test_8(id int PRIMARY KEY REFERENCES fkey_intermediate_schema_2.test_7(id));
    SELECT create_distributed_table('fkey_intermediate_schema_1.test_8', 'id');

    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_7'::regclass) ORDER BY 1;
    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_8'::regclass) ORDER BY 1;

    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_6'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_7'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_8'::regclass) ORDER BY 1;

    DROP SCHEMA fkey_intermediate_schema_1 CASCADE;

    SELECT get_referencing_relation_id_list::regclass FROM get_referencing_relation_id_list('test_7'::regclass) ORDER BY 1;
    SELECT get_referenced_relation_id_list::regclass FROM get_referenced_relation_id_list('test_7'::regclass) ORDER BY 1;

ROLLBACK;

CREATE OR REPLACE FUNCTION get_foreign_key_connected_relations(IN table_name regclass)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS 'citus', $$get_foreign_key_connected_relations$$;
COMMENT ON FUNCTION get_foreign_key_connected_relations(IN table_name regclass)
IS 'returns relations connected to input relation via a foreign key graph';

CREATE TABLE distributed_table_1(col int unique);
CREATE TABLE distributed_table_2(col int unique);
CREATE TABLE distributed_table_3(col int unique);
CREATE TABLE distributed_table_4(col int unique);

SELECT create_distributed_table('distributed_table_1', 'col');
SELECT create_distributed_table('distributed_table_2', 'col');
SELECT create_distributed_table('distributed_table_3', 'col');
SELECT create_distributed_table('distributed_table_4', 'col');

CREATE TABLE reference_table_1(col int unique);
CREATE TABLE reference_table_2(col int unique);

SELECT create_reference_table('reference_table_1');
SELECT create_reference_table('reference_table_2');


-- Now build below foreign key graph:
--
--                               --------------------------------------------
--                               ^                                          |
--                               |                                          v
-- distributed_table_1 <- distributed_table_2 -> reference_table_1 <- reference_table_2
--            |                   ^
--            |                   |
--            ----------> distributed_table_3

ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES distributed_table_1(col);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_2 FOREIGN KEY (col) REFERENCES reference_table_1(col);
ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_3 FOREIGN KEY (col) REFERENCES reference_table_1(col);
ALTER TABLE distributed_table_3 ADD CONSTRAINT fkey_4 FOREIGN KEY (col) REFERENCES distributed_table_2(col);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_5 FOREIGN KEY (col) REFERENCES reference_table_2(col);
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_6 FOREIGN KEY (col) REFERENCES distributed_table_3(col);

-- below queries should print all 5 tables mentioned in above graph

SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('reference_table_1') AS f(oid oid)
ORDER BY tablename;

SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('distributed_table_1') AS f(oid oid)
ORDER BY tablename;

-- show that this does not print anything as distributed_table_4
-- is not involved in any foreign key relationship
SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('distributed_table_4') AS f(oid oid)
ORDER BY tablename;

ALTER TABLE distributed_table_4 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES distributed_table_4(col);

-- show that we print table itself as it has a self reference
SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('distributed_table_4') AS f(oid oid)
ORDER BY tablename;

CREATE TABLE local_table_1 (col int unique);
CREATE TABLE local_table_2 (col int unique);

-- show that we trigger updating foreign key graph when
-- defining/dropping foreign keys between postgres tables

ALTER TABLE local_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES local_table_2(col);

SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('local_table_2') AS f(oid oid)
ORDER BY tablename;

ALTER TABLE local_table_1 DROP CONSTRAINT fkey_1;

SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('local_table_1') AS f(oid oid)
ORDER BY tablename;

-- show that we error out for non-existent tables
SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('non_existent_table') AS f(oid oid)
ORDER BY tablename;

\set VERBOSITY TERSE
SET client_min_messages TO DEBUG1;

BEGIN;
  ALTER TABLE distributed_table_2 DROP CONSTRAINT distributed_table_2_col_key CASCADE;
  ALTER TABLE distributed_table_3 DROP CONSTRAINT distributed_table_3_col_key CASCADE;
  -- show that we process drop constraint commands that are dropping uniquness
  -- constraints and then invalidate fkey graph. So we shouldn't see
  -- distributed_table_3 as it was split via above drop constraint commands
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('distributed_table_2') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;


-- Below two commands normally would invalidate foreign key graph.
-- But as they fail due to lacking of CASCADE, we don't invalidate
-- foreign key graph.
ALTER TABLE distributed_table_2 DROP CONSTRAINT distributed_table_2_col_key;
ALTER TABLE reference_table_2 DROP COLUMN col;

-- but we invalidate foreign key graph in below two transaction blocks
BEGIN;
  ALTER TABLE distributed_table_2 DROP CONSTRAINT distributed_table_2_col_key CASCADE;
ROLLBACK;

BEGIN;
  ALTER TABLE reference_table_2 DROP COLUMN col CASCADE;
ROLLBACK;

-- now we should see distributed_table_2 as well since we rollback'ed
SELECT oid::regclass::text AS tablename
FROM get_foreign_key_connected_relations('distributed_table_2') AS f(oid oid)
ORDER BY tablename;

BEGIN;
  DROP TABLE distributed_table_2 CASCADE;
  -- should only see reference_table_1 & reference_table_2
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('reference_table_1') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_55 FOREIGN KEY (col) REFERENCES reference_table_2(col);
  ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_66 FOREIGN KEY (col) REFERENCES distributed_table_3(col);
  -- show that we handle multiple edges between nodes in foreign key graph
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('reference_table_1') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  -- hide "verifying table" log because the order we print it changes
  -- in different pg versions
  set client_min_messages to error;
  ALTER TABLE distributed_table_2 ADD CONSTRAINT pkey PRIMARY KEY (col);
  set client_min_messages to debug1;

  -- show that droping a constraint not involved in any foreign key
  -- constraint doesn't invalidate foreign key graph
  ALTER TABLE distributed_table_2 DROP CONSTRAINT pkey;
ROLLBACK;

BEGIN;
  CREATE TABLE local_table_3 (col int PRIMARY KEY);
  ALTER TABLE local_table_1 ADD COLUMN another_col int REFERENCES local_table_3(col);

  CREATE TABLE local_table_4 (col int PRIMARY KEY REFERENCES local_table_3 (col));

  -- we invalidate foreign key graph for add column & create table
  -- commands defining foreign keys too
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('local_table_3') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  CREATE TABLE local_table_3 (col int PRIMARY KEY);
  ALTER TABLE local_table_1 ADD COLUMN another_col int REFERENCES local_table_3(col);

  ALTER TABLE local_table_1 DROP COLUMN another_col;

  -- we invalidate foreign key graph for drop column commands dropping
  -- referencing columns, should not print anything
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('local_table_1') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;

BEGIN;
  CREATE TABLE local_table_3 (col int PRIMARY KEY);
  ALTER TABLE local_table_1 ADD COLUMN another_col int REFERENCES local_table_3(col);

  ALTER TABLE local_table_3 DROP COLUMN col CASCADE;

  -- we invalidate foreign key graph for drop column commands dropping
  -- referenced columns, should not print anything
  SELECT oid::regclass::text AS tablename
  FROM get_foreign_key_connected_relations('local_table_1') AS f(oid oid)
  ORDER BY tablename;
ROLLBACK;

CREATE TABLE local_table_4 (col int);
-- Normally, we would decide to invalidate foreign key graph for below
-- ddl. But as it fails, we won't invalidate foreign key graph.
ALTER TABLE local_table_1 ADD COLUMN another_col int REFERENCES local_table_4(col);
-- When ddl command fails, we reset flag that we use to invalidate
-- foreign key graph so that next command doesn't invalidate foreign
-- key graph.
ALTER TABLE local_table_1 ADD COLUMN unrelated_column int;

-- show that droping a table not referenced and not referencing to any table
-- does not invalidate foreign key graph
DROP TABLE local_table_4;

set client_min_messages to error;

SET search_path TO public;
DROP SCHEMA fkey_graph CASCADE;
