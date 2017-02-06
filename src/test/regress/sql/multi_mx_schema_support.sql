--
-- MULTI_MX_SCHEMA_SUPPORT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1210000;

-- connect to a worker node and run some queries
\c - - - :worker_1_port

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


SELECT * FROM citus_mx_test_schema.nation_hash_collation_search_path;
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

-- set task_executor back to real-time
SET citus.task_executor_type TO "real-time";

