-- We create two sets of source and target tables, one set in Postgres and
-- the other in Citus distributed. We run the _exact_ MERGE SQL on both sets
-- and compare the final results of the target tables in Postgres and Citus.
-- The results should match. This process is repeated for various combinations
-- of MERGE SQL.

DROP SCHEMA IF EXISTS merge_repartition1_schema CASCADE;
CREATE SCHEMA merge_repartition1_schema;
SET search_path TO merge_repartition1_schema;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 5000000;
SET citus.explain_all_tasks TO true;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SET client_min_messages = warning;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;


CREATE TABLE pg_target(id int, val int);
CREATE TABLE pg_source(id int, val int, const int);
CREATE TABLE citus_target(id int, val int);
CREATE TABLE citus_source(id int, val int, const int);
SELECT citus_add_local_table_to_metadata('citus_target');
SELECT citus_add_local_table_to_metadata('citus_source');

CREATE OR REPLACE FUNCTION cleanup_data() RETURNS VOID SET search_path TO merge_repartition1_schema AS $$
    TRUNCATE pg_target;
    TRUNCATE pg_source;
    TRUNCATE citus_target;
    TRUNCATE citus_source;
    SELECT undistribute_table('citus_target');
    SELECT undistribute_table('citus_source');
$$
LANGUAGE SQL;
--
-- Load same set of data to both Postgres and Citus tables
--
CREATE OR REPLACE FUNCTION setup_data() RETURNS VOID SET search_path TO merge_repartition1_schema AS $$
    INSERT INTO pg_source SELECT i, i+1, 1 FROM generate_series(1, 10000) i;
    INSERT INTO pg_target SELECT i, 1 FROM generate_series(5001, 10000) i;
    INSERT INTO citus_source SELECT i, i+1, 1 FROM generate_series(1, 10000) i;
    INSERT INTO citus_target SELECT i, 1 FROM generate_series(5001, 10000) i;
$$
LANGUAGE SQL;

--
-- Compares the final target tables, merge-modified data, of both Postgres and Citus tables
--
CREATE OR REPLACE FUNCTION check_data(table1_name text, column1_name text, table2_name text, column2_name text)
RETURNS VOID SET search_path TO merge_repartition1_schema AS $$
DECLARE
    table1_avg numeric;
    table2_avg numeric;
BEGIN
    EXECUTE format('SELECT COALESCE(AVG(%I), 0) FROM %I', column1_name, table1_name) INTO table1_avg;
    EXECUTE format('SELECT COALESCE(AVG(%I), 0) FROM %I', column2_name, table2_name) INTO table2_avg;

    IF table1_avg > table2_avg THEN
        RAISE EXCEPTION 'The average of %.% is greater than %.%', table1_name, column1_name, table2_name, column2_name;
    ELSIF table1_avg < table2_avg THEN
        RAISE EXCEPTION 'The average of %.% is less than %.%', table1_name, column1_name, table2_name, column2_name;
    ELSE
        RAISE NOTICE 'The average of %.% is equal to %.%', table1_name, column1_name, table2_name, column2_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION compare_data() RETURNS VOID SET search_path TO merge_repartition1_schema AS $$
    SELECT check_data('pg_target', 'id', 'citus_target', 'id');
    SELECT check_data('pg_target', 'val', 'citus_target', 'val');
$$
LANGUAGE SQL;

--
-- Target and source are distributed, and non-colocated
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();

--
-- Target and source are distributed, and colocated but not joined on distribution column
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'citus_target');

MERGE INTO pg_target t
USING (SELECT * FROM pg_source) subq
ON (subq.val = t.id)
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = subq.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(subq.val, subq.id);

MERGE INTO citus_target t
USING (SELECT * FROM citus_source) subq
ON (subq.val = t.id)
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = subq.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(subq.val, subq.id);

SELECT compare_data();

--
-- Target and source are distributed, colocated, joined on distribution column
-- but with nondistribution values
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'citus_target');

MERGE INTO pg_target t
USING (SELECT id,const FROM pg_source UNION SELECT const,id FROM pg_source ) AS s
ON t.id = s.id
WHEN MATCHED THEN
        UPDATE SET val = s.const + 1
WHEN NOT MATCHED THEN
        INSERT VALUES(id, const);

MERGE INTO citus_target t
USING (SELECT id,const FROM citus_source UNION SELECT const,id FROM citus_source) AS s
ON t.id = s.id
WHEN MATCHED THEN
        UPDATE SET val = s.const + 1
WHEN NOT MATCHED THEN
        INSERT VALUES(id, const);

SELECT compare_data();

--
-- Repartition with a predicate on target_table_name rows in ON clause
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

MERGE INTO pg_target t
USING (SELECT * FROM pg_source WHERE id < 9500) s
ON t.id = s.id AND t.id < 9000
WHEN MATCHED AND t.id <= 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING (SELECT * FROM citus_source WHERE id < 9500) s
ON t.id = s.id AND t.id < 9000
WHEN MATCHED AND t.id <= 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT VALUES(s.id, s.val);

SELECT compare_data();

--
-- Test CTE and non-colocated tables
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

WITH cte AS (
        SELECT * FROM pg_source
)
MERGE INTO pg_target t
USING cte s
ON s.id = t.id
WHEN MATCHED AND t.id > 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES (s.id, s.val);

WITH cte AS (
        SELECT * FROM citus_source
)
MERGE INTO citus_target t
USING cte s
ON s.id = t.id
WHEN MATCHED AND t.id > 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES (s.id, s.val);

SELECT compare_data();

--
-- Test nested CTEs
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

WITH cte1 AS (
    SELECT * FROM pg_source ORDER BY 1 LIMIT 9000
),
cte2 AS(
    SELECT * FROM cte1
),
cte3 AS(
    SELECT * FROM cte2
)
MERGE INTO pg_target t
USING cte3 s
ON (s.id=t.id)
WHEN MATCHED AND t.id > 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT VALUES (s.id, s.val);

WITH cte1 AS (
    SELECT * FROM citus_source ORDER BY 1 LIMIT 9000
),
cte2 AS(
    SELECT * FROM cte1
),
cte3 AS(
    SELECT * FROM cte2
)
MERGE INTO citus_target t
USING cte3 s
ON (s.id=t.id)
WHEN MATCHED AND t.id > 7500 THEN
    UPDATE SET val = s.val + 1
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT VALUES (s.id, s.val);

SELECT compare_data();

--
-- Target and source are distributed and colocated
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with => 'citus_target');

MERGE INTO pg_target t
USING (SELECT 999 as newval, pg_source.* FROM (SELECT * FROM pg_source ORDER BY 1 LIMIT 6000) as src LEFT JOIN pg_source USING(id)) AS s
ON t.id = s.id
WHEN MATCHED AND t.id <= 5500 THEN
	UPDATE SET val = newval
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES(id, newval);

MERGE INTO citus_target t
USING (SELECT 999 as newval, citus_source.* FROM (SELECT * FROM citus_source ORDER BY 1 LIMIT 6000) as src LEFT JOIN citus_source USING(id)) AS s
ON t.id = s.id
WHEN MATCHED AND t.id <= 5500 THEN
	UPDATE SET val = newval
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES(id, newval);

SELECT compare_data();

--
-- Target is distributed and source is reference
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_reference_table('citus_source');

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();

--
-- Target is distributed and reference as source in a sub-query
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_reference_table('citus_source');

MERGE INTO pg_target t
USING (SELECT * FROM pg_source UNION SELECT * FROM pg_source) AS s ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + t.val
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING (SELECT * FROM citus_source UNION SELECT * FROM citus_source) AS s ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + t.val
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);
SELECT compare_data();

--
-- Target is distributed and citus-local as source
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT citus_add_local_table_to_metadata('citus_source');

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();

--
-- Target and source distributed and non-colocated. The source query requires evaluation
-- at the coordinator
--
SELECT cleanup_data();
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

MERGE INTO pg_target t
USING (SELECT 100 AS insval, MAX(const) AS updval, val, MAX(id) AS sid
	FROM pg_source
	GROUP BY val ORDER BY sid LIMIT 6000) AS s
ON t.id = s.sid
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = updval + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(sid, insval);

MERGE INTO citus_target t
USING (SELECT 100 AS insval, MAX(const) AS updval, val, MAX(id) AS sid
	FROM citus_source
	GROUP BY val ORDER BY sid LIMIT 6000) AS s
ON t.id = s.sid
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = updval + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(sid, insval);

SELECT compare_data();

-- Test source-query that requires repartitioning on top of MERGE repartitioning
SET client_min_messages TO WARNING;
SELECT cleanup_data();
RESET client_min_messages;
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');

MERGE INTO pg_target t
USING (SELECT s1.val FROM pg_source s1 JOIN pg_source s2 USING (val)) AS s
ON t.id = s.val
WHEN MATCHED THEN
      UPDATE SET val = t.val + 1;

SET citus.enable_repartition_joins TO true;
MERGE INTO citus_target t
USING (SELECT s1.val FROM citus_source s1 JOIN citus_source s2 USING (val)) AS s
ON t.id = s.val
WHEN MATCHED THEN
      UPDATE SET val = t.val + 1;

SELECT compare_data();

--
-- Test columnar as source table
--
SET client_min_messages TO WARNING;
SELECT cleanup_data();
RESET client_min_messages;
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'none');
SELECT alter_table_set_access_method('citus_source', 'columnar');

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED AND t.id <= 7500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();
SELECT alter_table_set_access_method('citus_source', 'heap');

-- Test CTE/Subquery in merge-actions (works only for router query)
SET client_min_messages TO WARNING;
SELECT cleanup_data();
RESET client_min_messages;
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with=>'citus_target');

MERGE INTO pg_target
USING pg_source
ON (pg_target.id = pg_source.id)
WHEN MATCHED AND (SELECT max_a > 5001 FROM (SELECT max(id) as max_a, max(val) as b FROM pg_target WHERE id = pg_source.id) AS foo) THEN
        DELETE
WHEN NOT MATCHED AND (SELECT max_a < 5001 FROM (SELECT max(id) as max_a, max(val) as b FROM pg_target WHERE id = pg_source.id) AS foo) THEN
        INSERT VALUES (pg_source.id, 100);

MERGE INTO citus_target
USING citus_source
ON (citus_target.id = citus_source.id)
WHEN MATCHED AND (SELECT max_a > 5001 FROM (SELECT max(id) as max_a, max(val) as b FROM citus_target WHERE id = citus_source.id) AS foo) THEN
        DELETE
WHEN NOT MATCHED AND (SELECT max_a < 5001 FROM (SELECT max(id) as max_a, max(val) as b FROM citus_target WHERE id = citus_source.id) AS foo) THEN
        INSERT VALUES (citus_source.id, 100);

SELECT compare_data();

--
-- Test target with false clause
--
SET client_min_messages TO WARNING;
SELECT cleanup_data();
RESET client_min_messages;
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with => 'citus_target');

MERGE INTO pg_target t
USING (SELECT * FROM pg_source WHERE id > 2500) AS s
ON t.id = s.id AND t.id < 2500
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING (SELECT * FROM citus_source WHERE id > 2500) AS s
ON t.id = s.id AND t.id < 2500
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();

SET client_min_messages TO WARNING;
SELECT cleanup_data();
RESET client_min_messages;
SELECT setup_data();
SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with => 'citus_target');

MERGE INTO pg_target t
USING (SELECT * FROM pg_source WHERE id = 2500) AS s
ON t.id = s.id AND t.id = 5000
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING (SELECT * FROM citus_source WHERE id = 2500) AS s
ON t.id = s.id AND t.id = 5000
WHEN MATCHED AND t.id <= 5500 THEN
        UPDATE SET val = s.val + 1
WHEN MATCHED THEN
        DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SELECT compare_data();

DROP SCHEMA merge_repartition1_schema CASCADE;
