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

SET search_path TO public;
DROP SCHEMA fkey_graph CASCADE;
