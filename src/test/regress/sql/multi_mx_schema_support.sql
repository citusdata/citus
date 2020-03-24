--
-- MULTI_MX_SCHEMA_SUPPORT
--

-- connect to a worker node and run some queries
\c - - :public_worker_1_host :worker_1_port

-- test very basic queries
SELECT * FROM nation_hash ORDER BY n_nationkey LIMIT 4;
SELECT * FROM citus_mx_test_schema.nation_hash ORDER BY n_nationkey LIMIT 4;


-- test cursors
SET search_path TO public;
BEGIN;
DECLARE test_cursor CURSOR FOR
    SELECT *
        FROM nation_hash
        WHERE n_nationkey = 1;
FETCH test_cursor;
FETCH test_cursor;
FETCH BACKWARD test_cursor;
END;

-- test with search_path is set
SET search_path TO citus_mx_test_schema;
BEGIN;
DECLARE test_cursor CURSOR FOR
    SELECT *
        FROM nation_hash
        WHERE n_nationkey = 1;
FETCH test_cursor;
FETCH test_cursor;
FETCH BACKWARD test_cursor;
END;


-- test inserting to table in different schema
SET search_path TO public;

INSERT INTO citus_mx_test_schema.nation_hash(n_nationkey, n_name, n_regionkey) VALUES (100, 'TURKEY', 3);

-- verify insertion
SELECT * FROM citus_mx_test_schema.nation_hash WHERE n_nationkey = 100;

-- test with search_path is set
SET search_path TO citus_mx_test_schema;

INSERT INTO nation_hash(n_nationkey, n_name, n_regionkey) VALUES (101, 'GERMANY', 3);

-- verify insertion
SELECT * FROM nation_hash WHERE n_nationkey = 101;

-- TODO: add UPDATE/DELETE/UPSERT


-- test UDFs with schemas
SET search_path TO public;


-- UDF in public, table in a schema other than public, search_path is not set
SELECT simpleTestFunction(n_nationkey)::int FROM citus_mx_test_schema.nation_hash GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

-- UDF in public, table in a schema other than public, search_path is set
SET search_path TO citus_mx_test_schema;
SELECT public.simpleTestFunction(n_nationkey)::int FROM citus_mx_test_schema.nation_hash GROUP BY 1 ORDER BY 1 DESC LIMIT 5;


-- UDF in schema, table in a schema other than public, search_path is not set
SET search_path TO public;
SELECT citus_mx_test_schema.simpleTestFunction2(n_nationkey)::int FROM citus_mx_test_schema.nation_hash  GROUP BY 1 ORDER BY 1 DESC LIMIT 5;

-- UDF in schema, table in a schema other than public, search_path is set
SET search_path TO citus_mx_test_schema;
SELECT simpleTestFunction2(n_nationkey)::int FROM nation_hash  GROUP BY 1 ORDER BY 1 DESC LIMIT 5;


-- test operators with schema
SET search_path TO public;

-- test with search_path is not set
SELECT * FROM citus_mx_test_schema.nation_hash  WHERE n_nationkey OPERATOR(citus_mx_test_schema.===) 1;

-- test with search_path is set
SET search_path TO citus_mx_test_schema;
SELECT * FROM nation_hash  WHERE n_nationkey OPERATOR(===) 1;


SELECT * FROM citus_mx_test_schema.nation_hash_collation_search_path ORDER BY 1;
SELECT n_comment FROM citus_mx_test_schema.nation_hash_collation_search_path ORDER BY n_comment COLLATE citus_mx_test_schema.english;

SET search_path  TO citus_mx_test_schema;

SELECT * FROM nation_hash_collation_search_path ORDER BY 1 DESC;
SELECT n_comment FROM nation_hash_collation_search_path ORDER BY n_comment COLLATE english;


SELECT * FROM citus_mx_test_schema.nation_hash_composite_types WHERE test_col = '(a,a)'::citus_mx_test_schema.new_composite_type ORDER BY 1::int DESC;

--test with search_path is set
SET search_path TO citus_mx_test_schema;
SELECT * FROM nation_hash_composite_types WHERE test_col = '(a,a)'::new_composite_type ORDER BY 1::int DESC;


-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    citus_mx_test_schema_join_1.nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is public,
-- join of two tables which are in same schemas,
-- join on partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    citus_mx_test_schema_join_1.nation_hash n1, citus_mx_test_schema_join_1.nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- single repartition joins
SET citus.task_executor_type TO "task-tracker";

-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on partition column and non-partition column
--SET search_path TO public;
SELECT
    count (*)
FROM
    citus_mx_test_schema_join_1.nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on partition column and non-partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on partition column and non-partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- hash repartition joins

-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on non-partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    citus_mx_test_schema_join_1.nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on non-partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, citus_mx_test_schema_join_2.nation_hash n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on non-partition column
SET search_path TO citus_mx_test_schema_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- set task_executor back to adaptive
SET citus.task_executor_type TO "adaptive";

-- connect to the master and do some test
-- regarding DDL support on schemas where
-- the search_path is set
\c - - :master_host :master_port

CREATE SCHEMA mx_ddl_schema_1;
CREATE SCHEMA mx_ddl_schema_2;
CREATE SCHEMA "CiTuS.TeAeN";

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

-- in the first test make sure that we handle DDLs
-- when search path is set
SET search_path TO mx_ddl_schema_1;
CREATE TABLE table_1 (key int PRIMARY KEY, value text);
SELECT create_distributed_table('table_1', 'key');
CREATE INDEX i1 ON table_1(value);
CREATE INDEX CONCURRENTLY i2 ON table_1(value);


-- now create a foriegn key on tables that are on seperate schemas
SET search_path TO mx_ddl_schema_1, mx_ddl_schema_2;
CREATE TABLE mx_ddl_schema_2.table_2 (key int PRIMARY KEY, value text);
SELECT create_distributed_table('mx_ddl_schema_2.table_2', 'key');

ALTER TABLE table_2 ADD CONSTRAINT test_constraint FOREIGN KEY (key) REFERENCES table_1(key);

-- we can also handle schema/table names with quotation
SET search_path TO "CiTuS.TeAeN";
CREATE TABLE "TeeNTabLE.1!?!"(id int, "TeNANt_Id" int);
SELECT create_distributed_table('"TeeNTabLE.1!?!"', 'id');

CREATE INDEX "MyTenantIndex" ON  "CiTuS.TeAeN"."TeeNTabLE.1!?!"("TeNANt_Id");
SET search_path TO "CiTuS.TeAeN", mx_ddl_schema_1, mx_ddl_schema_2;

ALTER TABLE "TeeNTabLE.1!?!" ADD CONSTRAINT test_constraint_2 FOREIGN KEY (id) REFERENCES table_1(key);

ALTER TABLE "TeeNTabLE.1!?!" ADD COLUMN new_col INT;

-- same semantics with CREATE INDEX CONCURRENTLY such that
-- it uses a single connection to execute all the commands
SET citus.multi_shard_modify_mode TO 'sequential';
ALTER TABLE "TeeNTabLE.1!?!" DROP COLUMN new_col;

-- set it back to the default value
SET citus.multi_shard_modify_mode TO 'parallel';

-- test with a not existing schema is in the search path
SET search_path TO not_existing_schema, "CiTuS.TeAeN";
ALTER TABLE "TeeNTabLE.1!?!" ADD COLUMN new_col INT;

-- test with a public schema is in the search path
SET search_path TO public, "CiTuS.TeAeN";
ALTER TABLE "TeeNTabLE.1!?!" DROP COLUMN new_col;

-- make sure that we handle transaction blocks properly
BEGIN;
    SET search_path TO public, "CiTuS.TeAeN";
    ALTER TABLE "TeeNTabLE.1!?!" ADD COLUMN new_col INT;

    SET search_path TO mx_ddl_schema_1;
    CREATE INDEX i55 ON table_1(value);

    SET search_path TO mx_ddl_schema_1, public, "CiTuS.TeAeN";
    ALTER TABLE "TeeNTabLE.1!?!" DROP COLUMN new_col;
    DROP INDEX i55;
COMMIT;

-- set the search_path to null
SET search_path TO '';
ALTER TABLE "CiTuS.TeAeN"."TeeNTabLE.1!?!" ADD COLUMN new_col INT;

-- set the search_path to not existing schema
SET search_path TO not_existing_schema;
ALTER TABLE "CiTuS.TeAeN"."TeeNTabLE.1!?!" DROP COLUMN new_col;

DROP SCHEMA mx_ddl_schema_1, mx_ddl_schema_2, "CiTuS.TeAeN" CASCADE;

-- test if ALTER TABLE SET SCHEMA sets the original table in the worker
SET search_path TO public;

CREATE SCHEMA mx_old_schema;
CREATE TABLE mx_old_schema.table_set_schema (id int);
SELECT create_distributed_table('mx_old_schema.table_set_schema', 'id');
CREATE SCHEMA mx_new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('mx_old_schema', 'mx_new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Table's Schema" FROM information_schema.tables WHERE table_name='table_set_schema';
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%'
    GROUP BY table_schema;
\c - - :master_host :master_port

ALTER TABLE mx_old_schema.table_set_schema SET SCHEMA mx_new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('mx_old_schema', 'mx_new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Table's Schema" FROM information_schema.tables WHERE table_name='table_set_schema';
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%'
    GROUP BY table_schema;
\c - - :master_host :master_port
SELECT * FROM mx_new_schema.table_set_schema;

DROP SCHEMA mx_old_schema CASCADE;
DROP SCHEMA mx_new_schema CASCADE;
