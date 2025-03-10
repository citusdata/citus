-- MERGE command performs a join from data_source to target_table_name
DROP SCHEMA IF EXISTS merge_schema CASCADE;
--MERGE INTO target
--USING source
--WHEN NOT MATCHED
--WHEN MATCHED AND <condition>
--WHEN MATCHED

CREATE SCHEMA merge_schema;
SET search_path TO merge_schema;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 4000000;
SET citus.explain_all_tasks TO true;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SET client_min_messages = warning;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

CREATE TABLE source
(
   order_id        INT,
   customer_id     INT,
   order_center    VARCHAR,
   order_time timestamp
);

CREATE TABLE target
(
   customer_id     INT,
   last_order_id   INT,
   order_center    VARCHAR,
   order_count     INT,
   last_order      timestamp
);

CREATE FUNCTION insert_data() RETURNS VOID AS $$

INSERT INTO source (order_id, customer_id, order_center, order_time)
   VALUES (101, 30000, 'WX', '2022-01-01 00:00:00'); -- Do not match
INSERT INTO source (order_id, customer_id, order_center, order_time)
   VALUES (102, 30001, 'CX', '2022-01-01 00:00:00'); -- Do not match

INSERT INTO source (order_id, customer_id, order_center, order_time)
   VALUES (103, 30002, 'AX', '2022-01-01 00:00:00'); -- Does match
INSERT INTO source (order_id, customer_id, order_center, order_time)
   VALUES (104, 30003, 'JX','2022-01-01 00:00:00' ); -- Does match
INSERT INTO source (order_id, customer_id, order_center, order_time)
   VALUES (105, 30004, 'JX','2022-01-01 00:00:00' ); -- Does match

INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (40000, 097, 'MK', -1, '2019-09-15 08:13:00');
INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (40001, 098, 'NU', -1, '2020-07-12 01:05:00');
INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (40002, 100, 'DS', -1, '2022-05-21 04:12:00');
INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (30002, 103, 'AX', -1, '2021-01-17 19:53:00'); -- Matches the source
INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (30003, 099, 'JX', -1, '2020-09-11 03:23:00'); -- Matches the source
INSERT INTO target (customer_id, last_order_id, order_center, order_count, last_order)
   VALUES (30004, 099, 'XX', -1, '2020-09-11 03:23:00'); -- Matches the source id AND the condition.
$$

LANGUAGE SQL;

SELECT insert_data();

SELECT 'Testing PG tables';

MERGE INTO target t
   USING source s
   ON (t.customer_id = s.customer_id)

   WHEN MATCHED AND t.order_center = 'XX' THEN
       DELETE

   WHEN MATCHED THEN
       UPDATE SET     -- Existing customer, update the order count and last_order_id
           order_count = t.order_count + 1,
           last_order_id = s.order_id

   WHEN NOT MATCHED THEN       -- New entry, record it.
       INSERT (customer_id, last_order_id, order_center, order_count, last_order)
           VALUES (customer_id, s.order_id, s.order_center, 123, s.order_time);

-- Our gold result to compare against
SELECT * INTO pg_result FROM target ORDER BY 1 ;

-- Clean the slate
TRUNCATE source;
TRUNCATE target;
SELECT insert_data();

-- Test with both target and source as Citus local

SELECT 'local - local';
SELECT citus_add_local_table_to_metadata('target');
SELECT citus_add_local_table_to_metadata('source');

MERGE INTO target t
   USING source s
   ON (t.customer_id = s.customer_id)

   WHEN MATCHED AND t.order_center = 'XX' THEN
       DELETE

   WHEN MATCHED THEN
       UPDATE SET     -- Existing customer, update the order count and last_order_id
           order_count = t.order_count + 1,
           last_order_id = s.order_id

   WHEN NOT MATCHED THEN       -- New entry, record it.
       INSERT (customer_id, last_order_id, order_center, order_count, last_order)
           VALUES (customer_id, s.order_id, s.order_center, 123, s.order_time);

SELECT * INTO local_local FROM target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM local_local c, pg_result p
WHERE c.customer_id = p.customer_id
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM pg_result p
WHERE NOT EXISTS (SELECT FROM local_local c WHERE c.customer_id = p.customer_id);


SELECT 'Testing Dist - Dist';

-- Clean the slate
TRUNCATE source;
TRUNCATE target;
SELECT insert_data();
SELECT undistribute_table('target');
SELECT undistribute_table('source');
SELECT create_distributed_table('target', 'customer_id');
SELECT create_distributed_table('source', 'customer_id', colocate_with=>'target');

-- Updates one of the row with customer_id  = 30002
SELECT * from target t WHERE t.customer_id  = 30002;
-- Turn on notice to print tasks sent to nodes
SET citus.log_remote_commands to true;
MERGE INTO target t
   USING source s
   ON (t.customer_id = s.customer_id) AND t.customer_id = 30002

   WHEN MATCHED AND t.order_center = 'XX' THEN
       DELETE

   WHEN MATCHED THEN
       UPDATE SET     -- Existing customer, update the order count and last_order_id
           order_count = t.order_count + 1,
           last_order_id = s.order_id

   WHEN NOT MATCHED THEN
       DO NOTHING;

SET citus.log_remote_commands to false;
SELECT * from target t WHERE t.customer_id  = 30002;

-- Deletes one of the row with customer_id  = 30004
SELECT * from target t WHERE t.customer_id  = 30004;
MERGE INTO target t
   USING source s
   ON (t.customer_id = s.customer_id) AND t.customer_id = 30004

   WHEN MATCHED AND t.order_center = 'XX' THEN
       DELETE

   WHEN MATCHED THEN
       UPDATE SET     -- Existing customer, update the order count and last_order_id
           order_count = t.order_count + 1,
           last_order_id = s.order_id

   WHEN NOT MATCHED THEN       -- New entry, record it.
       INSERT (customer_id, last_order_id, order_center, order_count, last_order)
           VALUES (customer_id, s.order_id, s.order_center, 123, s.order_time);
SELECT * from target t WHERE t.customer_id  = 30004;

-- Updating distribution column is allowed if the operation is a no-op
SELECT * from target t WHERE t.customer_id  = 30000;
MERGE INTO target t
USING SOURCE s
ON (t.customer_id = s.customer_id AND t.customer_id = 30000)
WHEN MATCHED THEN
	UPDATE SET customer_id = 30000;

MERGE INTO target t
USING SOURCE s
ON (t.customer_id = s.customer_id AND t.customer_id = 30000)
WHEN MATCHED THEN
	UPDATE SET customer_id = t.customer_id;
SELECT * from target t WHERE t.customer_id  = 30000;

--
-- Test MERGE with CTE as source
--
CREATE TABLE t1(id int, val int);
CREATE TABLE s1(id int, val int);

CREATE FUNCTION load() RETURNS VOID AS $$

INSERT INTO s1 VALUES(1, 0); -- Matches DELETE clause
INSERT INTO s1 VALUES(2, 1); -- Matches UPDATE clause
INSERT INTO s1 VALUES(3, 1); -- No Match INSERT clause
INSERT INTO s1 VALUES(4, 1); -- No Match INSERT clause
INSERT INTO s1 VALUES(6, 1); -- No Match INSERT clause

INSERT INTO t1 VALUES(1, 0); -- Will be deleted
INSERT INTO t1 VALUES(2, 0); -- Will be updated
INSERT INTO t1 VALUES(5, 0); -- Will be intact

$$
LANGUAGE SQL;

SELECT 'Testing PG tables';

SELECT load();

WITH pg_res AS (
	SELECT * FROM s1
)
MERGE INTO t1
	USING pg_res ON (pg_res.id = t1.id)

	WHEN MATCHED AND pg_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (pg_res.id, pg_res.val);

SELECT * FROM t1 order by id;
SELECT * INTO merge_result FROM t1 order by id;

-- Test Citus local tables

TRUNCATE t1;
TRUNCATE s1;
SELECT load();

SELECT citus_add_local_table_to_metadata('t1');
SELECT citus_add_local_table_to_metadata('s1');

WITH s1_res AS (
	SELECT * FROM s1
)
MERGE INTO t1
	USING s1_res ON (s1_res.id = t1.id)

	WHEN MATCHED AND s1_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1_res.id, s1_res.val);

-- Two rows with id 2 and val incremented, id 3, and id 1 is deleted
SELECT * FROM t1 order by id;

-- Should be empty
SELECT *
FROM merge_result p
WHERE NOT EXISTS (SELECT 1 FROM t1 c WHERE c.id = p.id AND c.val = p.val);


SELECT 'Testing dist - dist';

SELECT undistribute_table('t1');
SELECT undistribute_table('s1');
TRUNCATE t1;
TRUNCATE s1;
SELECT load();
SELECT create_distributed_table('t1', 'id');
SELECT create_distributed_table('s1', 'id', colocate_with=>'t1');


SELECT * FROM t1 order by id;
SET citus.log_remote_commands to true;
WITH s1_res AS (
	SELECT * FROM s1
)
MERGE INTO t1
	USING s1_res ON (s1_res.id = t1.id) AND t1.id = 6

	WHEN MATCHED AND s1_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1_res.id, s1_res.val);
SET citus.log_remote_commands to false;
-- Other than id 6 everything else is a NO match, and should appear in target
SELECT * FROM t1 order by 1, 2;

--
-- Test with multiple join conditions
--
CREATE TABLE t2(id int, val int, src text);
CREATE TABLE s2(id int, val int, src text);

CREATE OR REPLACE FUNCTION insert_data() RETURNS VOID AS $$
INSERT INTO t2 VALUES(1, 0, 'target');
INSERT INTO t2 VALUES(2, 0, 'target');
INSERT INTO t2 VALUES(3, 1, 'match');
INSERT INTO t2 VALUES(4, 0, 'match');

INSERT INTO s2 VALUES(2, 0, 'source'); -- No match insert
INSERT INTO s2 VALUES(4, 0, 'match'); -- Match delete
INSERT INTO s2 VALUES(3, 10, 'match'); -- Match update

$$
LANGUAGE SQL;

SELECT 'Testing PG tables';
SELECT insert_data();

MERGE INTO t2
USING s2
ON t2.id = s2.id AND t2.src = s2.src
	WHEN MATCHED AND t2.val = 1 THEN
		UPDATE SET val = s2.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (s2.id, s2.val, s2.src);

SELECT * FROM t2 ORDER BY 1;
SELECT * INTO pg_t2 FROM t2;

SELECT 'Testing Citus local tables';
TRUNCATE t2;
TRUNCATE s2;
SELECT insert_data();
SELECT citus_add_local_table_to_metadata('t2');
SELECT citus_add_local_table_to_metadata('s2');

MERGE INTO t2
USING s2
ON t2.id = s2.id AND t2.src = s2.src
	WHEN MATCHED AND t2.val = 1 THEN
		UPDATE SET val = s2.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (s2.id, s2.val, s2.src);

SELECT * FROM t2 ORDER BY 1;

-- Should be empty
SELECT *
FROM pg_t2 p
WHERE NOT EXISTS (SELECT 1 FROM t2 c WHERE c.id = p.id AND c.val = p.val AND c.src = p.src);

SELECT 'Testing Dist - Dist';
-- Clean the slate
TRUNCATE t2;
TRUNCATE s2;
SELECT insert_data();
SELECT undistribute_table('t2');
SELECT undistribute_table('s2');
SELECT create_distributed_table('t2', 'id');
SELECT create_distributed_table('s2', 'id', colocate_with => 't2');

SELECT * FROM t2 ORDER BY 1;
SET citus.log_remote_commands to true;
MERGE INTO t2
USING s2
ON t2.id = s2.id AND t2.src = s2.src AND t2.id = 4
	WHEN MATCHED AND t2.val = 1 THEN
		UPDATE SET val = s2.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		DO NOTHING;
SET citus.log_remote_commands to false;
-- Row with id = 4 is a match for delete clause, row should be deleted
-- Row with id = 3 is a NO match, row from source will be inserted
SELECT * FROM t2 ORDER BY 1;

--
-- With sub-query as the MERGE source
--
TRUNCATE t2;
TRUNCATE s2;
SELECT undistribute_table('t2');
SELECT undistribute_table('s2');
SELECT citus_add_local_table_to_metadata('t2');
SELECT citus_add_local_table_to_metadata('s2');
SELECT insert_data();

MERGE INTO t2 t
USING (SELECT * FROM s2) s
ON t.id = s.id AND t.src = s.src
	WHEN MATCHED AND t.val = 1 THEN
		UPDATE SET val = s.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (s.id, s.val, s.src);

SELECT * FROM t2 ORDER BY 1;
SELECT * INTO dist_res FROM t2 ORDER BY 1;

-- Should be equal
SELECT c.*, p.*
FROM t2 c, pg_t2 p
WHERE c.id = p.id AND c.src = p.src
ORDER BY 1,2;

-- Should be empty
SELECT *
FROM pg_t2 p
WHERE NOT EXISTS (SELECT 1 FROM t2 c WHERE c.id = p.id AND c.val = p.val AND c.src = p.src);


--
-- Using two source tables
--
CREATE TABLE t3(id int, val int, src text);
CREATE TABLE s3_1(id int, val int, src text);
CREATE TABLE s3_2(id int, val int, src text);

CREATE OR REPLACE FUNCTION insert_data() RETURNS VOID AS $$
INSERT INTO t3 VALUES(1, 0, 'target'); -- Intact
INSERT INTO t3 VALUES(2, 0, 'target');
INSERT INTO t3 VALUES(3, 0, 'target');
INSERT INTO t3 VALUES(5, 0, 'target'); -- Intact

INSERT INTO s3_1 VALUES(2, 0, 'source1');
INSERT INTO s3_1 VALUES(3, 0, 'source1');
INSERT INTO s3_1 VALUES(4, 0, 'source1');

INSERT INTO s3_2 VALUES(2, 1, 'source2'); -- Match update
INSERT INTO s3_2 VALUES(3, 0, 'source2'); -- Match delete
INSERT INTO s3_2 VALUES(4, 0, 'source2'); -- No match insert
INSERT INTO s3_2 VALUES(6, 0, 'source2'); -- Will miss the source-subquery-join
$$
LANGUAGE SQL;

SELECT insert_data();

MERGE INTO t3
	USING (SELECT s3_1.id, s3_2.val, s3_2.src FROM s3_1, s3_2 WHERE s3_1.id = s3_2.id) sub
	ON (t3.id = sub.id)
	WHEN MATCHED AND sub.val = 1 THEN
		UPDATE SET val = t3.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (sub.id, sub.val, sub.src);

-- Joining on columns inside the sub-query
MERGE INTO t3
	USING (SELECT s3_1.id, s3_2.val, s3_2.src FROM s3_1, s3_2 WHERE s3_1.id = s3_2.id) sub
	ON (t3.id = sub.id)
	WHEN MATCHED AND sub.val = 1 THEN
		UPDATE SET val = t3.val + 1
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (sub.id, sub.val, sub.src);

-- Constant Join condition
WITH s3_res AS (
	SELECT * FROM s3_1
)
MERGE INTO t3
	USING s3_res ON (FALSE)
	WHEN MATCHED AND s3_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t3.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (s3_res.id, s3_res.val, s3_res.src);

SELECT * FROM t3 ORDER BY 1,3;
SELECT * INTO pg_t3 FROM t3 ORDER BY 1;

SELECT 'Testing Local - Local';

TRUNCATE t3;
TRUNCATE s3_1;
TRUNCATE s3_2;
SELECT citus_add_local_table_to_metadata('t3');
SELECT citus_add_local_table_to_metadata('s3_1');
SELECT citus_add_local_table_to_metadata('s3_2');
SELECT insert_data();

MERGE INTO t3
	USING (SELECT s3_1.id, s3_2.val, s3_2.src FROM s3_1, s3_2 WHERE s3_1.id = s3_2.id) sub
	ON (t3.id = sub.id)
	WHEN MATCHED AND sub.val = 1 THEN
		UPDATE SET val = t3.val + 10
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (sub.id, sub.val, sub.src);

-- Joining on columns inside the sub-query
MERGE INTO t3
	USING (SELECT s3_1.id, s3_2.val, s3_2.src FROM s3_1, s3_2 WHERE s3_1.id = s3_2.id) sub
	ON (t3.id = sub.id)
	WHEN MATCHED AND sub.val = 1 THEN
		UPDATE SET val = t3.val + 1
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (sub.id, sub.val, sub.src);

-- Constant Join condition
WITH s3_res AS (
	SELECT * FROM s3_1
)
MERGE INTO t3
	USING s3_res ON (FALSE)
	WHEN MATCHED AND s3_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t3.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val, src) VALUES (s3_res.id, s3_res.val, s3_res.src);

SELECT * FROM t3 ORDER BY 1,3;
SELECT * INTO local_t3 FROM t3 ORDER BY 1;

-- Should be equal
SELECT c.*, p.*
FROM t3 c, pg_t3 p
WHERE c.id = p.id
ORDER BY 1,2;

-- Should be empty
SELECT *
FROM pg_t3 p
WHERE NOT EXISTS (SELECT 1 FROM local_t3 c WHERE c.id = p.id AND c.val = p.val AND c.src = p.src);

--
-- Test table functions
--

CREATE TABLE tf_target(first_name varchar, last_name varchar, eid text);

WITH doc as (
SELECT '<people>
    <person>
        <first_name>foo</first_name>
        <last_name>bar</last_name>
        <eid>100</eid>
    </person>
    <person>
        <first_name>moo</first_name>
        <last_name>bar</last_name>
        <eid>200</eid>
    </person>
</people>'::xml as source_xml
)

MERGE INTO tf_target
USING (
SELECT decoded.*
FROM doc, xmltable(
        '//people/person'
        passing source_xml
        columns
            first_name text,
            last_name text,
            eid text) as decoded) as tf_source
ON tf_source.first_name = tf_target.first_name
WHEN NOT MATCHED THEN
INSERT VALUES (tf_source.first_name, tf_source.last_name, tf_source.eid);

-- Our gold result to compare against
SELECT * INTO tf_result FROM tf_target ORDER BY 1 ;

TRUNCATE tf_target;
SELECT citus_add_local_table_to_metadata('tf_target');

WITH doc as (
SELECT '<people>
    <person>
        <first_name>foo</first_name>
        <last_name>bar</last_name>
        <eid>100</eid>
    </person>
    <person>
        <first_name>moo</first_name>
        <last_name>bar</last_name>
        <eid>200</eid>
    </person>
</people>'::xml as source_xml
)

MERGE INTO tf_target
USING (
SELECT decoded.*
FROM doc, xmltable(
        '//people/person'
        passing source_xml
        columns
            first_name text,
            last_name text,
            eid text) as decoded) as tf_source
ON tf_source.first_name = tf_target.first_name
WHEN NOT MATCHED THEN
INSERT VALUES (tf_source.first_name, tf_source.last_name, tf_source.eid);

SELECT * INTO tf_local FROM tf_target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM tf_local c, tf_result p
WHERE c.eid = p.eid
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM tf_result p
WHERE NOT EXISTS (SELECT FROM tf_local c WHERE c.eid = p.eid);

--
-- Test VALUES RTE type
--
CREATE TABLE vl_target(id int, value varchar);
INSERT INTO vl_target VALUES(100, 'target');

MERGE INTO vl_target
USING (SELECT *
        FROM (VALUES(100, 'source1'), (200, 'source2')) AS vl (ID, value)) as vl_source
ON vl_source.ID = vl_target.ID
WHEN MATCHED THEN
UPDATE SET value = vl_source.value, id = vl_target.id + 1
WHEN NOT MATCHED THEN
INSERT VALUES(vl_source.ID, vl_source.value);

-- Our gold result to compare against
SELECT * INTO vl_result FROM vl_target ORDER BY 1 ;

-- Clean the slate
TRUNCATE vl_target;
INSERT INTO vl_target VALUES(100, 'target');
SELECT citus_add_local_table_to_metadata('vl_target');

SET client_min_messages TO DEBUG1;
MERGE INTO vl_target
USING (SELECT *
        FROM (VALUES(100, 'source1'), (200, 'source2')) AS vl (ID, value)) as vl_source
ON vl_source.ID = vl_target.ID
WHEN MATCHED THEN
UPDATE SET value = vl_source.value, id = vl_target.id + 1
WHEN NOT MATCHED THEN
INSERT VALUES(vl_source.ID, vl_source.value);
RESET client_min_messages;

SELECT * INTO vl_local FROM vl_target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM vl_local c, vl_result p
WHERE c.id = p.id
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM vl_result p
WHERE NOT EXISTS (SELECT FROM vl_local c WHERE c.id = p.id);


--
-- Test function scan
--
CREATE FUNCTION f_immutable(i integer) RETURNS INTEGER AS
$$ BEGIN RETURN i; END; $$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE TABLE rs_target(id int);

MERGE INTO rs_target
USING (SELECT * FROM f_immutable(99) id WHERE id in (SELECT 99)) AS rs_source
ON rs_source.id = rs_target.id
WHEN MATCHED THEN
DO NOTHING
WHEN NOT MATCHED THEN
INSERT VALUES(rs_source.id);

-- Our gold result to compare against
SELECT * INTO rs_result FROM rs_target ORDER BY 1 ;

-- Clean the slate
TRUNCATE rs_target;
SELECT citus_add_local_table_to_metadata('rs_target');

SET client_min_messages TO DEBUG1;
MERGE INTO rs_target
USING (SELECT * FROM f_immutable(99) id WHERE id in (SELECT 99)) AS rs_source
ON rs_source.id = rs_target.id
WHEN MATCHED THEN
DO NOTHING
WHEN NOT MATCHED THEN
INSERT VALUES(rs_source.id);
RESET client_min_messages;

SELECT * INTO rs_local FROM rs_target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM rs_local c, rs_result p
WHERE c.id = p.id
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM rs_result p
WHERE NOT EXISTS (SELECT FROM rs_local c WHERE c.id = p.id);

--
-- Test Materialized view
--

CREATE TABLE mv_target(id int, val varchar);
CREATE TABLE mv_source_table(id int, val varchar);

INSERT INTO mv_source_table VALUES(1, 'src1');
INSERT INTO mv_source_table VALUES(2, 'src2');

CREATE MATERIALIZED VIEW mv_source AS
SELECT * FROM mv_source_table;

MERGE INTO mv_target
USING mv_source
ON mv_source.id = mv_target.id
WHEN MATCHED THEN
    DO NOTHING
WHEN NOT MATCHED THEN
    INSERT VALUES(mv_source.id, mv_source.val);

-- Our gold result to compare against
SELECT * INTO mv_result FROM mv_target ORDER BY 1 ;

-- Clean the slate
TRUNCATE mv_target;
SELECT citus_add_local_table_to_metadata('mv_target');
SELECT citus_add_local_table_to_metadata('mv_source_table');
DROP MATERIALIZED VIEW mv_source;
CREATE MATERIALIZED VIEW mv_source AS
SELECT * FROM mv_source_table;

MERGE INTO mv_target
USING mv_source
ON mv_source.id = mv_target.id
WHEN MATCHED THEN
    DO NOTHING
WHEN NOT MATCHED THEN
    INSERT VALUES(mv_source.id, mv_source.val);

SELECT * INTO mv_local FROM mv_target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM mv_local c, mv_result p
WHERE c.id = p.id
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM mv_result p
WHERE NOT EXISTS (SELECT FROM mv_local c WHERE c.id = p.id);

--
-- Distributed table as source (indirect)
--
CREATE TABLE dist_table(id int, source varchar);
INSERT INTO dist_table VALUES(2, 'dist_table');
INSERT INTO dist_table VALUES(3, 'dist_table');
INSERT INTO dist_table VALUES(100, 'dist_table');

CREATE FUNCTION f_dist() returns SETOF RECORD AS
$$
BEGIN
RETURN QUERY SELECT id, source FROM dist_table;
END;
$$ language plpgsql volatile;

CREATE TABLE fn_target(id int, data varchar);

MERGE INTO fn_target
--USING (SELECT * FROM f_dist() f(id integer, source varchar)) as fn_source
USING (SELECT id, source FROM dist_table) as fn_source
ON fn_source.id = fn_target.id
WHEN MATCHED THEN
DO NOTHING
WHEN NOT MATCHED THEN
INSERT VALUES(fn_source.id, fn_source.source);

-- Our gold result to compare against
SELECT * INTO fn_result FROM fn_target ORDER BY 1 ;

-- Clean the slate
TRUNCATE TABLE fn_target;
SELECT citus_add_local_table_to_metadata('fn_target');
SELECT citus_add_local_table_to_metadata('dist_table');

SET client_min_messages TO DEBUG1;
MERGE INTO fn_target
--USING (SELECT * FROM f_dist() f(id integer, source varchar)) as fn_source
USING (SELECT id, source FROM dist_table) as fn_source
ON fn_source.id = fn_target.id
WHEN MATCHED THEN
DO NOTHING
WHEN NOT MATCHED THEN
INSERT VALUES(fn_source.id, fn_source.source);
RESET client_min_messages;

SELECT * INTO fn_local FROM fn_target ORDER BY 1 ;

-- Should be equal
SELECT c.*, p.*
FROM fn_local c, fn_result p
WHERE c.id = p.id
ORDER BY 1,2;

-- Must return zero rows
SELECT *
FROM fn_result p
WHERE NOT EXISTS (SELECT FROM fn_local c WHERE c.id = p.id);

--
-- Foreign tables
--
CREATE TABLE ft_target (id integer NOT NULL, user_val varchar);
CREATE TABLE ft_source (id integer NOT NULL, user_val varchar);
SELECT citus_add_local_table_to_metadata('ft_source');

INSERT INTO ft_target VALUES (1, 'target');
INSERT INTO ft_target VALUES (2, 'target');

INSERT INTO ft_source VALUES (2, 'source');
INSERT INTO ft_source VALUES (3, 'source');

SELECT * FROM ft_target;

CREATE EXTENSION postgres_fdw;

CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');

CREATE USER MAPPING FOR CURRENT_USER
        SERVER foreign_server
        OPTIONS (user 'postgres');

CREATE FOREIGN TABLE foreign_table (
        id integer NOT NULL,
        user_val text
)
        SERVER foreign_server
        OPTIONS (schema_name 'merge_schema', table_name 'ft_source');

SELECT citus_add_local_table_to_metadata('foreign_table');

-- Foreign table as source
SET client_min_messages TO DEBUG1;
MERGE INTO ft_target
	USING foreign_table ON (foreign_table.id = ft_target.id)
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, user_val) VALUES (foreign_table.id, foreign_table.user_val);
RESET client_min_messages;

SELECT * FROM ft_target;

--
-- complex joins on the source side
--

-- source(join of two relations) relation is an unaliased join

CREATE TABLE target_cj(tid int, src text, val int);
CREATE TABLE source_cj1(sid1 int, src1 text, val1 int);
CREATE TABLE source_cj2(sid2 int, src2 text, val2 int);

INSERT INTO target_cj VALUES (1, 'target', 0);
INSERT INTO target_cj VALUES (2, 'target', 0);
INSERT INTO target_cj VALUES (2, 'target', 0);
INSERT INTO target_cj VALUES (3, 'target', 0);

INSERT INTO source_cj1 VALUES (2, 'source-1', 10);
INSERT INTO source_cj2 VALUES (2, 'source-2', 20);

BEGIN;
MERGE INTO target_cj t
USING source_cj1 s1 INNER JOIN source_cj2 s2 ON sid1 = sid2
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src2
WHEN NOT MATCHED THEN
        DO NOTHING;
-- Gold result to compare against
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

BEGIN;
-- try accessing columns from either side of the source join
MERGE INTO target_cj t
USING source_cj1 s2
        INNER JOIN source_cj2 s1 ON sid1 = sid2 AND val1 = 10
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET tid = sid2, src = src1, val = val2
WHEN NOT MATCHED THEN
        DO NOTHING;
-- Gold result to compare against
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

-- Test the same scenarios with distributed tables

SELECT create_distributed_table('target_cj', 'tid');
SELECT create_distributed_table('source_cj1', 'sid1', colocate_with => 'target_cj');
SELECT create_distributed_table('source_cj2', 'sid2', colocate_with => 'target_cj');

BEGIN;
MERGE INTO target_cj t
USING (SELECT * FROM source_cj1 s1 INNER JOIN source_cj2 s2 ON sid1 = sid2) s
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src2
WHEN NOT MATCHED THEN
        DO NOTHING;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

BEGIN;
-- try accessing columns from either side of the source join
MERGE INTO target_cj t
USING (SELECT * FROM source_cj1 s2
        INNER JOIN source_cj2 s1 ON sid1 = sid2 AND val1 = 10) s
ON t.tid = sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src1, val = val2
WHEN NOT MATCHED THEN
        DO NOTHING;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

-- sub-query as a source
BEGIN;
MERGE INTO target_cj t
USING (SELECT * FROM source_cj1 WHERE sid1 = 2) sub
ON t.tid = sub.sid1 AND t.tid = 2
WHEN MATCHED THEN
	UPDATE SET src = sub.src1, val = val1
WHEN NOT MATCHED THEN
	DO NOTHING;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

-- Test self-join
BEGIN;
SELECT * FROM target_cj ORDER BY 1;
set citus.log_remote_commands to true;
MERGE INTO target_cj t1
USING (SELECT * FROM target_cj) sub
ON t1.tid = sub.tid AND t1.tid = 3
WHEN MATCHED THEN
	UPDATE SET src = sub.src, val = sub.val + 100
WHEN NOT MATCHED THEN
	DO NOTHING;
set citus.log_remote_commands to false;
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;


-- Test PREPARE
PREPARE merge_prepare(int) AS
MERGE INTO target_cj target
USING (SELECT * FROM source_cj1) sub
ON target.tid = sub.sid1 AND target.tid = $1
WHEN MATCHED THEN
        UPDATE SET val = sub.val1
WHEN NOT MATCHED THEN
        DO NOTHING;

SELECT * FROM target_cj ORDER BY 1;

BEGIN;
EXECUTE merge_prepare(2);
EXECUTE merge_prepare(2);
EXECUTE merge_prepare(2);
EXECUTE merge_prepare(2);
EXECUTE merge_prepare(2);
SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

BEGIN;

SET citus.log_remote_commands to true;
SET client_min_messages TO DEBUG1;
EXECUTE merge_prepare(2);
RESET client_min_messages;

EXECUTE merge_prepare(2);
SET citus.log_remote_commands to false;

SELECT * FROM target_cj ORDER BY 1;
ROLLBACK;

-- Test distributed tables, must be co-located and joined on distribution column.

--
-- We create two sets of source and target tables, one set is Postgres and the other
-- is Citus distributed. Run the _exact_ MERGE SQL on both the sets and compare the
-- final results of target tables of Postgres and Citus, the result should match.
-- This is repeated for various MERGE SQL combinations
--
CREATE TABLE pg_target(id int, val varchar);
CREATE TABLE pg_source(id int, val varchar);
CREATE TABLE citus_target(id int, val varchar);
CREATE TABLE citus_source(id int, val varchar);

-- Half of the source rows do not match
INSERT INTO pg_target SELECT i, 'target' FROM generate_series(250, 500) i;
INSERT INTO pg_source SELECT i, 'source' FROM generate_series(1, 500) i;

INSERT INTO citus_target SELECT i, 'target' FROM generate_series(250, 500) i;
INSERT INTO citus_source SELECT i, 'source' FROM generate_series(1, 500) i;

SELECT create_distributed_table('citus_target', 'id');
SELECT create_distributed_table('citus_source', 'id', colocate_with => 'citus_target');

--
-- This routine compares the target tables of Postgres and Citus and
-- returns true if they match, false if the results do not match.
--
CREATE OR REPLACE FUNCTION compare_tables() RETURNS BOOLEAN AS $$
DECLARE ret BOOL;
BEGIN
SELECT count(1) = 0 INTO ret
    FROM pg_target
    FULL OUTER JOIN citus_target
        USING (id, val)
    WHERE pg_target.id IS NULL
        OR citus_target.id IS NULL;
RETURN ret;
END
$$ LANGUAGE PLPGSQL;

-- Make sure we start with exact data in Postgres and Citus
SELECT compare_tables();

-- Run the MERGE on both Postgres and Citus, and compare the final target tables

BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

--
-- ON clause filter on source
--
BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id AND s.id < 100
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id AND s.id < 100
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

--
-- ON clause filter on target
--
BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id AND t.id < 100
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id AND t.id < 100
WHEN MATCHED AND t.id > 400 THEN
	UPDATE SET val = t.val || 'Updated by Merge'
WHEN MATCHED THEN
	DELETE
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

--
-- NOT MATCHED clause filter on source
--
BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id
WHEN MATCHED THEN
	DO NOTHING
WHEN NOT MATCHED AND s.id < 100 THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN MATCHED THEN
	DO NOTHING
WHEN NOT MATCHED AND s.id < 100 THEN
        INSERT VALUES(s.id, s.val);

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

--
-- Test constant filter in ON clause to check if shards are pruned
-- with restriction information
--

--
-- Though constant filter is present, this won't prune shards as
-- NOT MATCHED clause is present
--
BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id AND s.id = 250
WHEN MATCHED THEN
        UPDATE SET val = t.val || 'Updated by Merge'
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id AND s.id = 250
WHEN MATCHED THEN
        UPDATE SET val = t.val || 'Updated by Merge'
WHEN NOT MATCHED THEN
        INSERT VALUES(s.id, s.val);

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;


-- let's create source and target table
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 13000;
CREATE TABLE source_pushdowntest (id integer);
CREATE TABLE target_pushdowntest (id integer );

-- let's distribute both table on id field
SELECT create_distributed_table('source_pushdowntest', 'id');
SELECT create_distributed_table('target_pushdowntest', 'id');

-- we are doing this operation on single node setup let's figure out colocation id of both tables
-- both has same colocation id so both are colocated.
WITH colocations AS (
    SELECT colocationid
    FROM pg_dist_partition
    WHERE logicalrelid = 'source_pushdowntest'::regclass
       OR logicalrelid = 'target_pushdowntest'::regclass
)
SELECT
    CASE
        WHEN COUNT(DISTINCT colocationid) = 1 THEN 'Same'
        ELSE 'Different'
    END AS colocation_status
FROM colocations;

SET client_min_messages TO DEBUG1;
-- Test 1 : tables are colocated AND query is multisharded AND Join On distributed column : should push down to workers.

EXPLAIN (costs off, timing off, summary off)
MERGE INTO target_pushdowntest t
USING source_pushdowntest s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id)
  VALUES (s.id);

-- Test 2 : tables are colocated AND source query is not multisharded : should push down to worker.
-- DEBUG LOGS show that query is getting pushed down
MERGE INTO target_pushdowntest t
USING (SELECT * from source_pushdowntest where id = 1) s
on t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id)
  VALUES (s.id);


-- Test 3 : tables are colocated source query is single sharded but not using source distributed column in insertion. let's not pushdown.
INSERT INTO source_pushdowntest (id) VALUES (3);

EXPLAIN (costs off, timing off, summary off)
MERGE INTO target_pushdowntest t
USING (SELECT 1 as somekey, id from source_pushdowntest where id = 1) s
on t.id = s.somekey
WHEN NOT MATCHED THEN
  INSERT (id)
  VALUES (s.somekey);


-- let's verify if we use some other column from source for value of distributed column in target.
-- it should be inserted to correct shard of target.
CREATE TABLE source_withdata (id integer, some_number integer);
CREATE TABLE target_table (id integer, name text);
SELECT create_distributed_table('source_withdata', 'id');
SELECT create_distributed_table('target_table', 'id');

INSERT INTO source_withdata (id, some_number) VALUES (1, 3);

-- we will use some_number column from source_withdata to insert into distributed column of target.
-- value of some_number is 3 let's verify what shard it should go to.
select worker_hash(3);

-- it should go to second shard of target as target has 4 shard and hash "-28094569" comes in range of second shard.
MERGE INTO target_table t
USING (SELECT id, some_number from source_withdata where id = 1) s
on t.id = s.some_number
WHEN NOT MATCHED THEN
  INSERT (id, name)
  VALUES (s.some_number, 'parag');

-- let's verify if data inserted to second shard of target.
EXPLAIN (analyze on, costs off, timing off, summary off) SELECT * FROM target_table;

-- let's verify target data too.
SELECT * FROM target_table;


-- test UPDATE : when source is single sharded and table are colocated
MERGE INTO target_table t
USING (SELECT id, some_number from source_withdata where id = 1) s
on t.id = s.some_number
WHEN MATCHED THEN
  UPDATE SET name = 'parag jain';

-- let's verify if data updated properly.
SELECT * FROM target_table;

-- let's see what happend when we try to update distributed key of target table
MERGE INTO target_table t
USING (SELECT id, some_number from source_withdata where id = 1) s
on t.id = s.some_number
WHEN MATCHED THEN
  UPDATE SET id = 1500;

SELECT * FROM target_table;

-- test DELETE : when source is single sharded and table are colocated
MERGE INTO target_table t
USING (SELECT id, some_number from source_withdata where id = 1) s
on t.id = s.some_number
WHEN MATCHED THEN
  DELETE;

-- let's verify if data deleted properly.
SELECT * FROM target_table;

--
DELETE FROM source_withdata;
DELETE FROM target_table;
INSERT INTO source VALUES (1,1);

merge into target_table sda
using source_withdata sdn
on sda.id = sdn.id AND sda.id = 1
when not matched then
  insert (id)
  values (10000);

SELECT * FROM target_table WHERE id = 10000;

RESET client_min_messages;



-- This will prune shards with restriction information as NOT MATCHED is void
BEGIN;
SET citus.log_remote_commands to true;

MERGE INTO pg_target t
USING pg_source s
ON t.id = s.id AND s.id = 250
WHEN MATCHED THEN
        UPDATE SET val = t.val || 'Updated by Merge'
WHEN NOT MATCHED THEN
        DO NOTHING;

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id AND s.id = 250
WHEN MATCHED THEN
        UPDATE SET val = t.val || 'Updated by Merge'
WHEN NOT MATCHED THEN
        DO NOTHING;

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

-- Test CTE with distributed tables
CREATE VIEW pg_source_view AS SELECT * FROM pg_source WHERE id < 400;
CREATE VIEW citus_source_view AS SELECT * FROM citus_source WHERE id < 400;

BEGIN;
SEt citus.log_remote_commands to true;

WITH cte AS (
        SELECT * FROM pg_source_view
)
MERGE INTO pg_target t
USING cte
ON cte.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated by CTE'
WHEN NOT MATCHED THEN
        INSERT VALUES (cte.id, cte.val)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

WITH cte AS (
        SELECT * FROM citus_source_view
)
MERGE INTO citus_target t
USING cte
ON cte.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated by CTE'
WHEN NOT MATCHED THEN
        INSERT VALUES (cte.id, cte.val)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;


-- Test sub-query with distributed tables
BEGIN;
SEt citus.log_remote_commands to true;

MERGE INTO pg_target t
USING (SELECT * FROM pg_source) subq
ON subq.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated by subquery'
WHEN NOT MATCHED THEN
        INSERT VALUES (subq.id, subq.val)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

MERGE INTO citus_target t
USING (SELECT * FROM citus_source) subq
ON subq.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated by subquery'
WHEN NOT MATCHED THEN
        INSERT VALUES (subq.id, subq.val)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

SET citus.log_remote_commands to false;
SELECT compare_tables();
ROLLBACK;

-- Test PREPARE
PREPARE pg_prep(int) AS
MERGE INTO pg_target
USING (SELECT * FROM pg_source) sub
ON pg_target.id = sub.id AND pg_target.id = $1
WHEN MATCHED THEN
        UPDATE SET val = 'Updated by prepare using ' || sub.val
WHEN NOT MATCHED THEN
        INSERT VALUES (sub.id, sub.val);

PREPARE citus_prep(int) AS
MERGE INTO citus_target
USING (SELECT * FROM citus_source) sub
ON citus_target.id = sub.id AND citus_target.id = $1
WHEN MATCHED THEN
        UPDATE SET val = 'Updated by prepare using ' || sub.val
WHEN NOT MATCHED THEN
        INSERT VALUES (sub.id, sub.val);

BEGIN;

SELECT * FROM pg_target WHERE id = 500; -- before merge
SELECT count(*) FROM pg_target; -- before merge
EXECUTE pg_prep(500);
SELECT * FROM pg_target WHERE id = 500; -- non-cached
EXECUTE pg_prep(500);
EXECUTE pg_prep(500);
EXECUTE pg_prep(500);
EXECUTE pg_prep(500);
EXECUTE pg_prep(500);
SELECT * FROM pg_target WHERE id = 500; -- cached
SELECT count(*) FROM pg_target; -- cached

SELECT * FROM citus_target WHERE id = 500; -- before merge
SELECT count(*) FROM citus_target; -- before merge
SET citus.log_remote_commands to true;
EXECUTE citus_prep(500);
SELECT * FROM citus_target WHERE id = 500; -- non-cached
EXECUTE citus_prep(500);
EXECUTE citus_prep(500);
EXECUTE citus_prep(500);
EXECUTE citus_prep(500);
EXECUTE citus_prep(500);
SET citus.log_remote_commands to false;
SELECT * FROM citus_target WHERE id = 500; -- cached
SELECT count(*) FROM citus_target; -- cached

SELECT compare_tables();
ROLLBACK;

-- Test partitions + distributed tables

CREATE TABLE pg_pa_target (tid integer, balance float, val text)
	PARTITION BY LIST (tid);
CREATE TABLE citus_pa_target (tid integer, balance float, val text)
	PARTITION BY LIST (tid);

CREATE TABLE part1 PARTITION OF pg_pa_target FOR VALUES IN (1,4)
  WITH (autovacuum_enabled=off);
CREATE TABLE part2 PARTITION OF pg_pa_target FOR VALUES IN (2,5,6)
  WITH (autovacuum_enabled=off);
CREATE TABLE part3 PARTITION OF pg_pa_target FOR VALUES IN (3,8,9)
  WITH (autovacuum_enabled=off);
CREATE TABLE part4 PARTITION OF pg_pa_target DEFAULT
  WITH (autovacuum_enabled=off);
CREATE TABLE part5 PARTITION OF citus_pa_target FOR VALUES IN (1,4)
  WITH (autovacuum_enabled=off);
CREATE TABLE part6 PARTITION OF citus_pa_target FOR VALUES IN (2,5,6)
  WITH (autovacuum_enabled=off);
CREATE TABLE part7 PARTITION OF citus_pa_target FOR VALUES IN (3,8,9)
  WITH (autovacuum_enabled=off);
CREATE TABLE part8 PARTITION OF citus_pa_target DEFAULT
  WITH (autovacuum_enabled=off);

CREATE TABLE pg_pa_source (sid integer, delta float);
CREATE TABLE citus_pa_source (sid integer, delta float);

-- insert many rows to the source table
INSERT INTO pg_pa_source SELECT id, id * 10  FROM generate_series(1,14) AS id;
INSERT INTO citus_pa_source SELECT id, id * 10  FROM generate_series(1,14) AS id;
-- insert a few rows in the target table (odd numbered tid)
INSERT INTO pg_pa_target SELECT id, id * 100, 'initial' FROM generate_series(1,14,2) AS id;
INSERT INTO citus_pa_target SELECT id, id * 100, 'initial' FROM generate_series(1,14,2) AS id;

SELECT create_distributed_table('citus_pa_target', 'tid');
SELECT create_distributed_table('citus_pa_source', 'sid');

CREATE OR REPLACE FUNCTION pa_compare_tables() RETURNS BOOLEAN AS $$
DECLARE ret BOOL;
BEGIN
SELECT count(1) = 0 INTO ret
    FROM pg_pa_target
    FULL OUTER JOIN citus_pa_target
        USING (tid, balance, val)
    WHERE pg_pa_target.tid IS NULL
        OR citus_pa_target.tid IS NULL;
RETURN ret;
END
$$ LANGUAGE PLPGSQL;

-- try simple MERGE
BEGIN;
MERGE INTO pg_pa_target t
  USING pg_pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');

MERGE INTO citus_pa_target t
  USING citus_pa_source s
  ON t.tid = s.sid
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');

SELECT pa_compare_tables();
ROLLBACK;

-- same with a constant qual
BEGIN;
MERGE INTO pg_pa_target t
  USING pg_pa_source s
  ON t.tid = s.sid AND tid = 1
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');

MERGE INTO citus_pa_target t
  USING citus_pa_source s
  ON t.tid = s.sid AND tid = 1
  WHEN MATCHED THEN
    UPDATE SET balance = balance + delta, val = val || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid, delta, 'inserted by merge');

SELECT pa_compare_tables();
ROLLBACK;

CREATE TABLE source_json( id   integer, z int, d jsonb);
CREATE TABLE target_json( id   integer, z int, d jsonb);

INSERT INTO source_json SELECT i,i FROM generate_series(0,5)i;

SELECT create_distributed_table('target_json','id'), create_distributed_table('source_json', 'id');

-- single shard query given source_json is filtered and Postgres is smart to pushdown
-- filter to the target_json as well
SELECT public.coordinator_plan($Q$
EXPLAIN (ANALYZE ON, TIMING OFF) MERGE INTO target_json sda
USING (SELECT * FROM source_json WHERE id = 1) sdn
ON sda.id = sdn.id
WHEN NOT matched THEN
	INSERT (id, z) VALUES (sdn.id, 5);
$Q$);
SELECT * FROM target_json ORDER BY 1;

-- zero shard query as filters do not match
--SELECT public.coordinator_plan($Q$
--EXPLAIN (ANALYZE ON, TIMING OFF) MERGE INTO target_json sda
--USING (SELECT * FROM source_json WHERE id = 1) sdn
--ON sda.id = sdn.id AND sda.id = 2
--WHEN NOT matched THEN
--	INSERT (id, z) VALUES (sdn.id, 5);
--$Q$);
--SELECT * FROM target_json ORDER BY 1;

-- join for source_json is happening at a different place
SELECT public.coordinator_plan($Q$
EXPLAIN (ANALYZE ON, TIMING OFF) MERGE INTO target_json sda
USING source_json s1 LEFT JOIN (SELECT * FROM source_json) s2 USING(z)
ON sda.id = s1.id AND s1.id = s2.id
WHEN NOT matched THEN
	INSERT (id, z) VALUES (s2.id, 5);
$Q$);
SELECT * FROM target_json ORDER BY 1;

-- update JSON column
SELECT public.coordinator_plan($Q$
EXPLAIN (ANALYZE ON, TIMING OFF) MERGE INTO target_json sda
USING source_json sdn
ON sda.id = sdn.id
WHEN matched THEN
	UPDATE SET d = '{"a" : 5}';
$Q$);
SELECT * FROM target_json ORDER BY 1;

CREATE FUNCTION immutable_hash(int) RETURNS int
AS 'SELECT hashtext( ($1 + $1)::text);'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

MERGE INTO target_json sda
USING source_json sdn
ON sda.id = sdn.id
WHEN matched THEN
	UPDATE SET z = immutable_hash(sdn.z);

-- Test bigserial
CREATE TABLE source_serial (id integer, z int, d bigserial);
CREATE TABLE target_serial (id integer, z int, d bigserial);
INSERT INTO source_serial SELECT i,i FROM generate_series(0,100)i;
SELECT create_distributed_table('source_serial', 'id'),
       create_distributed_table('target_serial', 'id');

MERGE INTO target_serial sda
USING source_serial sdn
ON sda.id = sdn.id
WHEN NOT matched THEN
       INSERT (id, z) VALUES (id, z);

SELECT count(*) from source_serial;
SELECT count(*) from target_serial;

SELECT count(distinct d) from source_serial;
SELECT count(distinct d) from target_serial;

-- Test set operations
CREATE TABLE target_set(t1 int, t2 int);
CREATE TABLE source_set(s1 int, s2 int);

SELECT create_distributed_table('target_set', 't1'),
       create_distributed_table('source_set', 's1');

INSERT INTO target_set VALUES(1, 0);
INSERT INTO source_set VALUES(1, 1);
INSERT INTO source_set VALUES(2, 2);

MERGE INTO target_set
USING (SELECT * FROM source_set UNION SELECT * FROM source_set) AS foo ON target_set.t1 = foo.s1
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.s1);
SELECT * FROM target_set ORDER BY 1, 2;

--
-- Reference as a source
--
CREATE TABLE reftarget_local(t1 int, t2 int);
CREATE TABLE refsource_ref(s1 int, s2 int);

INSERT INTO reftarget_local VALUES(1, 0);
INSERT INTO reftarget_local VALUES(3, 100);
INSERT INTO refsource_ref VALUES(1, 1);
INSERT INTO refsource_ref VALUES(2, 2);
INSERT INTO refsource_ref VALUES(3, 3);

MERGE INTO reftarget_local
USING (SELECT * FROM refsource_ref UNION SELECT * FROM refsource_ref) AS foo ON reftarget_local.t1 = foo.s1
WHEN MATCHED AND reftarget_local.t2 = 100 THEN
	DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.s1);

DROP TABLE IF EXISTS pg_result;
SELECT * INTO pg_result FROM reftarget_local ORDER BY 1, 2;

-- Make source table as reference (target is Postgres)
TRUNCATE reftarget_local;
TRUNCATE refsource_ref;
INSERT INTO reftarget_local VALUES(1, 0);
INSERT INTO reftarget_local VALUES(3, 100);
INSERT INTO refsource_ref VALUES(1, 1);
INSERT INTO refsource_ref VALUES(2, 2);
INSERT INTO refsource_ref VALUES(3, 3);
SELECT create_reference_table('refsource_ref');

MERGE INTO reftarget_local
USING (SELECT * FROM refsource_ref UNION SELECT * FROM refsource_ref) AS foo ON reftarget_local.t1 = foo.s1
WHEN MATCHED AND reftarget_local.t2 = 100 THEN
	DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.s1);
SELECT * INTO pg_ref FROM reftarget_local ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM pg_ref c, pg_result p
WHERE c.t1 = p.t1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_result FULL OUTER JOIN pg_ref ON pg_result.t1 = pg_ref.t1
WHERE pg_result.t1 IS NULL OR pg_ref.t1 IS NULL;

-- Now make both Citus tables, reference as source, local as target
TRUNCATE reftarget_local;
TRUNCATE refsource_ref;
INSERT INTO reftarget_local VALUES(1, 0);
INSERT INTO reftarget_local VALUES(3, 100);
INSERT INTO refsource_ref VALUES(1, 1);
INSERT INTO refsource_ref VALUES(2, 2);
INSERT INTO refsource_ref VALUES(3, 3);

SELECT citus_add_local_table_to_metadata('reftarget_local');

MERGE INTO reftarget_local
USING (SELECT * FROM refsource_ref UNION SELECT * FROM refsource_ref) AS foo ON reftarget_local.t1 = foo.s1
WHEN MATCHED AND reftarget_local.t2 = 100 THEN
	DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.s1);
SELECT * INTO local_ref FROM reftarget_local ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM local_ref c, pg_result p
WHERE c.t1 = p.t1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_result FULL OUTER JOIN local_ref ON pg_result.t1 = local_ref.t1
WHERE pg_result.t1 IS NULL OR local_ref.t1 IS NULL;

-- Now make target as distributed, keep reference as source
TRUNCATE reftarget_local;
TRUNCATE refsource_ref;
INSERT INTO reftarget_local VALUES(1, 0);
INSERT INTO reftarget_local VALUES(3, 100);
INSERT INTO refsource_ref VALUES(1, 1);
INSERT INTO refsource_ref VALUES(2, 2);
INSERT INTO refsource_ref VALUES(3, 3);

SELECT create_distributed_table('reftarget_local', 't1');

MERGE INTO reftarget_local
USING (SELECT * FROM refsource_ref UNION SELECT * FROM refsource_ref) AS foo ON reftarget_local.t1 = foo.s1
WHEN MATCHED AND reftarget_local.t2 = 100 THEN
	DELETE
WHEN MATCHED THEN
        UPDATE SET t2 = t2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.s1);
SELECT * INTO dist_reftarget FROM reftarget_local ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM dist_reftarget c, pg_result p
WHERE c.t1 = p.t1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_result FULL OUTER JOIN dist_reftarget ON pg_result.t1 = dist_reftarget.t1
WHERE pg_result.t1 IS NULL OR dist_reftarget.t1 IS NULL;

--
-- Distributed (target), Reference(source)
--
CREATE TABLE demo_distributed(id1 int, val1 int);
CREATE TABLE demo_source_table(id2 int, val2 int);

CREATE FUNCTION setup_demo_data() RETURNS VOID AS $$
INSERT INTO demo_distributed VALUES(1, 100);
INSERT INTO demo_distributed VALUES(7, 100);
INSERT INTO demo_distributed VALUES(15, 100);
INSERT INTO demo_distributed VALUES(100, 0);
INSERT INTO demo_distributed VALUES(300, 100);
INSERT INTO demo_distributed VALUES(400, 0);

INSERT INTO demo_source_table VALUES(1, 77);
INSERT INTO demo_source_table VALUES(15, 77);
INSERT INTO demo_source_table VALUES(75, 77);
INSERT INTO demo_source_table VALUES(100, 77);
INSERT INTO demo_source_table VALUES(300, 77);
INSERT INTO demo_source_table VALUES(400, 77);
INSERT INTO demo_source_table VALUES(500, 77);
$$
LANGUAGE SQL;

CREATE FUNCTION merge_demo_data() RETURNS VOID AS $$
MERGE INTO demo_distributed t
USING demo_source_table s ON s.id2 = t.id1
WHEN MATCHED AND t.val1= 0 THEN
	DELETE
WHEN MATCHED THEN
	UPDATE SET val1 = val1 + s.val2
WHEN NOT MATCHED THEN
	INSERT VALUES(s.id2, s.val2);
$$
LANGUAGE SQL;

SELECT setup_demo_data();
SELECT merge_demo_data();
SELECT * INTO pg_demo_result FROM demo_distributed ORDER BY 1, 2;

TRUNCATE demo_distributed;
TRUNCATE demo_source_table;

SELECT create_distributed_table('demo_distributed', 'id1');
SELECT create_reference_table('demo_source_table');

SELECT setup_demo_data();
SELECT merge_demo_data();

SELECT * INTO dist_demo_result FROM demo_distributed ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM dist_demo_result c, pg_demo_result p
WHERE c.id1 = p.id1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_demo_result p FULL OUTER JOIN dist_demo_result d ON p.id1 = d.id1
WHERE p.id1 IS NULL OR d.id1 IS NULL;

-- Now convert source as distributed, but non-colocated with target
DROP TABLE pg_demo_result, dist_demo_result;
SELECT undistribute_table('demo_distributed');
SELECT undistribute_table('demo_source_table');

CREATE OR REPLACE FUNCTION merge_demo_data() RETURNS VOID AS $$
MERGE INTO demo_distributed t
USING (SELECT id2,val2 FROM demo_source_table UNION SELECT val2,id2 FROM demo_source_table) AS s
ON t.id1 = s.id2
WHEN MATCHED THEN
        UPDATE SET val1 = val1 + 1;
$$
LANGUAGE SQL;

TRUNCATE demo_distributed;
TRUNCATE demo_source_table;

SELECT setup_demo_data();
SELECT merge_demo_data();
SELECT * INTO pg_demo_result FROM demo_distributed ORDER BY 1, 2;

SELECT create_distributed_table('demo_distributed', 'id1');
SELECT create_distributed_table('demo_source_table', 'id2', colocate_with=>'none');

TRUNCATE demo_distributed;
TRUNCATE demo_source_table;

SELECT setup_demo_data();
SELECT merge_demo_data();
SELECT * INTO dist_demo_result FROM demo_distributed ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM dist_demo_result c, pg_demo_result p
WHERE c.id1 = p.id1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_demo_result p FULL OUTER JOIN dist_demo_result d ON p.id1 = d.id1
WHERE p.id1 IS NULL OR d.id1 IS NULL;

-- Test with LIMIT

CREATE OR REPLACE FUNCTION merge_demo_data() RETURNS VOID AS $$
MERGE INTO demo_distributed t
USING (SELECT 999 as s3, demo_source_table.* FROM (SELECT * FROM demo_source_table ORDER BY 1 LIMIT 3) as foo LEFT JOIN demo_source_table USING(id2)) AS s
ON t.id1 = s.id2
WHEN MATCHED THEN
	UPDATE SET val1 = s3
WHEN NOT MATCHED THEN
	INSERT VALUES(id2, s3);
$$
LANGUAGE SQL;

DROP TABLE pg_demo_result, dist_demo_result;
SELECT undistribute_table('demo_distributed');
SELECT undistribute_table('demo_source_table');

TRUNCATE demo_distributed;
TRUNCATE demo_source_table;

SELECT setup_demo_data();
SELECT merge_demo_data();
SELECT * INTO pg_demo_result FROM demo_distributed ORDER BY 1, 2;

SELECT create_distributed_table('demo_distributed', 'id1');
SELECT create_distributed_table('demo_source_table', 'id2', colocate_with=>'none');

TRUNCATE demo_distributed;
TRUNCATE demo_source_table;

SELECT setup_demo_data();
SELECT merge_demo_data();
SELECT * INTO dist_demo_result FROM demo_distributed ORDER BY 1, 2;

-- Should be equal
SELECT c.*, p.*
FROM dist_demo_result c, pg_demo_result p
WHERE c.id1 = p.id1
ORDER BY 1,2;

-- Must return zero rows
SELECT count(*)
FROM pg_demo_result p FULL OUTER JOIN dist_demo_result d ON p.id1 = d.id1
WHERE p.id1 IS NULL OR d.id1 IS NULL;

-- Test explain with repartition
SET citus.explain_all_tasks TO false;
EXPLAIN (COSTS OFF)
MERGE INTO demo_distributed t
USING (SELECT 999 as s3, demo_source_table.* FROM (SELECT * FROM demo_source_table ORDER BY 1 LIMIT 3) as foo LEFT JOIN demo_source_table USING(id2)) AS s
ON t.id1 = s.id2
WHEN MATCHED THEN
	UPDATE SET val1 = s3
WHEN NOT MATCHED THEN
	INSERT VALUES(id2, s3);

-- Test multiple join conditions on distribution column
MERGE INTO demo_distributed t
USING (SELECT id2+1 as key, id2+3 as key2 FROM demo_source_table) s
ON t.id1 = s.key2 ANd t.id1 = s.key
WHEN NOT MATCHED THEN
	INSERT VALUES(s.key2, 333);

MERGE INTO demo_distributed t
USING (SELECT id2+1 as key, id2+2 as key2 FROM demo_source_table) s
ON t.id1 = s.key2 AND t.id1 = s.key
WHEN NOT MATCHED THEN
	DO NOTHING;

MERGE INTO demo_distributed t
USING (SELECT id2+1 as key, id2+3 as key2 FROM demo_source_table) s
ON t.val1 = s.key2 AND t.id1 = s.key AND t.id1 = s.key2
WHEN NOT MATCHED THEN
	INSERT VALUES(s.key2, 444);

-- Test aggregate functions in source-query
SELECT COUNT(*) FROM demo_distributed where val1 = 150;
SELECT COUNT(*) FROM demo_distributed where id1 = 2;

-- One row with Key=7 updated in demo_distributed to 150
MERGE INTO demo_distributed t
USING (SELECT count(DISTINCT id2)::int4 as key FROM demo_source_table GROUP BY val2) s
ON t.id1 = s.key
WHEN NOT MATCHED THEN INSERT VALUES(s.key, 1)
WHEN MATCHED THEN UPDATE SET val1 = 150;

-- Seven rows with Key=2 inserted in demo_distributed
MERGE INTO demo_distributed t
USING (SELECT (count(DISTINCT val2) + 1)::int4 as key FROM demo_source_table GROUP BY id2) s
ON t.id1 = s.key
WHEN NOT MATCHED THEN INSERT VALUES(s.key, 1)
WHEN MATCHED THEN UPDATE SET val1 = 150;

SELECT COUNT(*) FROM demo_distributed where val1 = 150;
SELECT COUNT(*) FROM demo_distributed where id1 = 2;

--
-- Test FALSE filters
--
CREATE TABLE source_filter(order_id INT, customer_id INT, order_center VARCHAR, order_time timestamp);
CREATE TABLE target_filter(customer_id INT, last_order_id INT, order_center VARCHAR, order_count INT, last_order timestamp);

SELECT create_distributed_table('source_filter', 'customer_id');
SELECT create_distributed_table('target_filter', 'customer_id', colocate_with => 'source_filter');

CREATE FUNCTION load_filter() RETURNS VOID AS $$

TRUNCATE target_filter;
TRUNCATE source_filter;

INSERT INTO target_filter VALUES(100, 11, 'trg', -1, '2022-01-01 00:00:00'); -- Match UPDATE
INSERT INTO target_filter VALUES(200, 11, 'trg', -1, '2022-01-01 00:00:00'); -- Match DELETE

INSERT INTO source_filter VALUES(12, 100, 'src', '2022-01-01 00:00:00');
INSERT INTO source_filter VALUES(12, 200, 'src', '2022-01-01 00:00:00');
INSERT INTO source_filter VALUES(12, 300, 'src', '2022-01-01 00:00:00');

$$
LANGUAGE SQL;

--WHEN MATCH and FALSE
SELECT load_filter();
MERGE INTO target_filter t
USING source_filter s
ON s.customer_id = t.customer_id
WHEN MATCHED AND t.customer_id = 100 AND (FALSE) THEN
	UPDATE SET order_count = 999
WHEN MATCHED AND t.customer_id = 200 THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES(s.customer_id, s.order_id, s.order_center, 1, s.order_time);

SELECT * FROM target_filter ORDER BY 1, 2;

--WHEN NOT MATCH and 1=0
SELECT load_filter();
MERGE INTO target_filter t
USING source_filter s
ON s.customer_id = t.customer_id
WHEN MATCHED AND t.customer_id = 100 THEN
	UPDATE SET order_count = 999
WHEN MATCHED AND t.customer_id = 200 THEN
	DELETE
WHEN NOT MATCHED AND (1=0) THEN
	INSERT VALUES(s.customer_id, s.order_id, s.order_center, 1, s.order_time);

SELECT * FROM target_filter ORDER BY 1, 2;

--ON t.key = s.key AND 1 < 0
SELECT load_filter();
MERGE INTO target_filter t
USING source_filter s
ON s.customer_id = t.customer_id AND 1 < 0
WHEN MATCHED AND t.customer_id = 100 THEN
	UPDATE SET order_count = 999
WHEN MATCHED AND t.customer_id = 200 THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES(s.customer_id, s.order_id, s.order_center, 1, s.order_time);

SELECT * FROM target_filter ORDER BY 1, 2;

--(SELECT * FROM source_filter WHERE false) as source_filter
SELECT load_filter();
MERGE INTO target_filter t
USING (SELECT * FROM source_filter WHERE false) s
ON s.customer_id = t.customer_id
WHEN MATCHED AND t.customer_id = 100 THEN
	UPDATE SET order_count = 999
WHEN MATCHED AND t.customer_id = 200 THEN
	DELETE
WHEN NOT MATCHED THEN
	INSERT VALUES(s.customer_id, s.order_id, s.order_center, 1, s.order_time);

SELECT * FROM target_filter ORDER BY 1, 2;

-- Bug 6785
CREATE TABLE source_6785( id   integer, z int, d jsonb);
CREATE TABLE target_6785( id   integer, z int, d jsonb);
SELECT create_distributed_table('target_6785','id'), create_distributed_table('source_6785', 'id');
INSERT INTO source_6785 SELECT i,i FROM generate_series(0,5)i;

SET client_min_messages TO DEBUG1;
MERGE INTO target_6785 sda
USING (SELECT * FROM source_6785 WHERE id = 1) sdn
ON sda.id = sdn.id AND sda.id = 2
WHEN NOT matched THEN
      INSERT (id, z) VALUES (sdn.id, 5);
RESET client_min_messages;

SELECT * FROM target_6785 ORDER BY 1;

--
-- Error and Unsupported scenarios
--


-- Test explain analyze with repartition
EXPLAIN ANALYZE
MERGE INTO demo_distributed t
USING (SELECT 999 as s3, demo_source_table.* FROM (SELECT * FROM demo_source_table ORDER BY 1 LIMIT 3) as foo LEFT JOIN demo_source_table USING(id2)) AS s
ON t.id1 = s.id2
WHEN MATCHED THEN
	UPDATE SET val1 = s3
WHEN NOT MATCHED THEN
	INSERT VALUES(id2, s3);

-- Source without a table
MERGE INTO target_cj t
USING (VALUES (1, 1), (2, 1), (3, 3)) as s (sid, val)
ON t.tid = s.sid AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET val = s.val
WHEN NOT MATCHED THEN
        DO NOTHING;

-- Incomplete source
MERGE INTO target_cj t
USING (source_cj1 s1 INNER JOIN source_cj2 s2 ON sid1 = val2) s
ON t.tid = s.sid1 AND t.tid = 2
WHEN MATCHED THEN
        UPDATE SET src = src2
WHEN NOT MATCHED THEN
        DO NOTHING;

-- Reference as a target and local as source
MERGE INTO refsource_ref
USING (SELECT * FROM reftarget_local UNION SELECT * FROM reftarget_local) AS foo ON refsource_ref.s1 = foo.t1
WHEN MATCHED THEN
        UPDATE SET s2 = s2 + 100
WHEN NOT MATCHED THEN
	INSERT VALUES(foo.t1);

MERGE INTO target_set
USING source_set AS foo ON target_set.t1 = foo.s1
WHEN MATCHED THEN
        UPDATE SET ctid = '(0,100)';

-- modifying CTE not supported
EXPLAIN
WITH cte_1 AS (DELETE FROM target_json RETURNING *)
MERGE INTO target_json sda
USING cte_1 sdn
ON sda.id = sdn.id
WHEN NOT matched THEN
	INSERT (id, z) VALUES (sdn.id, 5);

-- Grouping sets not supported
MERGE INTO citus_target t
USING (SELECT count(*), id FROM citus_source GROUP BY GROUPING SETS (id, val)) subq
ON subq.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated'
WHEN NOT MATCHED THEN
        INSERT VALUES (subq.id, 99)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

WITH subq AS
(
SELECT count(*), id FROM citus_source GROUP BY GROUPING SETS (id, val)
)
MERGE INTO citus_target t
USING subq
ON subq.id = t.id
WHEN MATCHED AND t.id > 350 THEN
    UPDATE SET val = t.val || 'Updated'
WHEN NOT MATCHED THEN
        INSERT VALUES (subq.id, 99)
WHEN MATCHED AND t.id < 350 THEN
        DELETE;

-- try inserting unmatched distribution column value
MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT DEFAULT VALUES;

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT VALUES(10000);

MERGE INTO citus_target t
USING citus_source s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id) VALUES(1000);

-- Colocated merge
MERGE INTO t1 t
USING s1 s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (id) VALUES(s.val);

MERGE INTO t1 t
USING s1 s
ON t.id = s.id
WHEN NOT MATCHED THEN
  INSERT (val) VALUES(s.val);

-- Non-colocated merge
MERGE INTO t1 t
USING s1 s
ON t.id = s.val
WHEN NOT MATCHED THEN
  INSERT (id) VALUES(s.id);

-- try updating the distribution key column
BEGIN;
MERGE INTO target_cj t
  USING source_cj1 s
  ON t.tid = s.sid1 AND t.tid = 2
  WHEN MATCHED THEN
    UPDATE SET tid = tid + 9, src = src || ' updated by merge'
  WHEN NOT MATCHED THEN
    INSERT VALUES (sid1, 'inserted by merge', val1);
ROLLBACK;

-- Foreign table as target
MERGE INTO foreign_table
	USING ft_target ON (foreign_table.id = ft_target.id)
	WHEN MATCHED THEN
		DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, user_val) VALUES (ft_target.id, ft_target.user_val);

TRUNCATE t1;
TRUNCATE s1;
SELECT undistribute_table('t1');
SELECT undistribute_table('s1');
SELECT citus_add_local_table_to_metadata('t1');
SELECT create_distributed_table('s1', 'id');
SELECT load();

-- Combination of Citus local table and distributed table
MERGE INTO t1
	USING s1 ON (s1.id = t1.val) -- val is not a distribution column
	WHEN MATCHED AND s1.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1.id, s1.val);

-- Now both s1 and t1 are distributed tables
SELECT undistribute_table('t1');
SELECT create_distributed_table('t1', 'id');

-- We have a potential pitfall where a function can be invoked in
-- the MERGE conditions which can insert/update to a random shard
CREATE OR REPLACE function merge_when_and_write() RETURNS BOOLEAN
LANGUAGE PLPGSQL AS
$$
BEGIN
        INSERT INTO t1 VALUES (100, 100);
        RETURN TRUE;
END;
$$;

-- Test functions executing in MERGE statement. This is to prevent the functions from
-- doing a random sql, which may be executed in a remote node or modifying the target
-- relation which will have unexpected/suprising results.
MERGE INTO t1 USING (SELECT * FROM s1 WHERE true) s1 ON
  t1.id = s1.id AND s1.id = 2
   WHEN matched THEN
 UPDATE SET id = s1.id, val = random();

-- Test STABLE function
CREATE FUNCTION add_s(integer, integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL
STABLE RETURNS NULL ON NULL INPUT;

MERGE INTO t1
USING s1 ON t1.id = s1.id
WHEN NOT MATCHED THEN
	INSERT VALUES(s1.id, add_s(s1.val, 2));

-- Test preventing "ON" join condition from writing to the database
BEGIN;
MERGE INTO t1
USING s1 ON t1.id = s1.id AND t1.id = 2 AND (merge_when_and_write())
WHEN MATCHED THEN
        UPDATE SET val = t1.val + s1.val;
ROLLBACK;

-- Test preventing WHEN clause(s) from writing to the database
BEGIN;
MERGE INTO t1
USING s1 ON t1.id = s1.id AND t1.id = 2
WHEN MATCHED AND (merge_when_and_write()) THEN
        UPDATE SET val = t1.val + s1.val;
ROLLBACK;


-- Joining on non-partition columns with CTE source, but INSERT incorrect column
WITH s1_res AS (
	SELECT * FROM s1
)
MERGE INTO t1
	USING s1_res ON (s1_res.val = t1.id)
	WHEN MATCHED AND s1_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1_res.id, s1_res.val);

-- Constant Join condition
WITH s1_res AS (
	SELECT * FROM s1
)
MERGE INTO t1
	USING s1_res ON (TRUE)
	WHEN MATCHED AND s1_res.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1_res.id, s1_res.val);

-- Join condition without target distribution column
WITH s1_res AS (
     SELECT * FROM s1
 )
 MERGE INTO t1 USING s1_res ON (s1_res.id = t1.val)
 WHEN MATCHED THEN DELETE
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1_res.id, s1_res.val);


--
-- Reference tables
--
SELECT undistribute_table('t1');
SELECT undistribute_table('s1');
SELECT create_reference_table('t1');
SELECT create_reference_table('s1');

MERGE INTO t1
	USING s1 ON (s1.id = t1.id)

	WHEN MATCHED AND s1.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1.id, s1.val);


--
-- Postgres + Citus-Distributed table
--
SELECT undistribute_table('t1');
SELECT undistribute_table('s1');
SELECT create_distributed_table('t1', 'id');

MERGE INTO t1
	USING s1 ON (s1.id = t1.id)
	WHEN MATCHED AND s1.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1.id, s1.val);

MERGE INTO t1
	USING (SELECT * FROM s1) sub ON (sub.id = t1.id)
	WHEN MATCHED AND sub.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (sub.id, sub.val);

CREATE TABLE pg(val int);
SELECT create_distributed_table('s1', 'id');

-- Both t1 and s1 are citus distributed tables now, mix Postgres table in sub-query
MERGE INTO t1
	USING (SELECT s1.id, pg.val FROM s1, pg) sub ON (sub.id = t1.id)
	WHEN MATCHED AND sub.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (sub.id, sub.val);

-- Mix Postgres table in CTE
WITH pg_res AS (
	SELECT * FROM pg
)
MERGE INTO t1
	USING (SELECT s1.id, pg_res.val FROM s1, pg_res) sub ON (sub.id = t1.id)
	WHEN MATCHED AND sub.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (sub.id, sub.val);

-- Match more than one source row should fail same as Postgres behavior
SELECT undistribute_table('t1');
SELECT undistribute_table('s1');
SELECT citus_add_local_table_to_metadata('t1');
SELECT citus_add_local_table_to_metadata('s1');

INSERT INTO s1 VALUES(1, 1); -- From load(), we already have row with id = 1

MERGE INTO t1
	USING s1 ON (s1.id = t1.id)

	WHEN MATCHED AND s1.val = 0 THEN
		DELETE
	WHEN MATCHED THEN
		UPDATE SET val = t1.val + 1
	WHEN NOT MATCHED THEN
		INSERT (id, val) VALUES (s1.id, s1.val);

-- Materialized view as target is not allowed
MERGE INTO mv_source
USING mv_target
ON mv_source.id = mv_target.id
WHEN MATCHED THEN
    DO NOTHING
WHEN NOT MATCHED THEN
    INSERT VALUES(mv_source.id, mv_source.val);

-- Do not allow constant values into the distribution column
CREATE TABLE dist_target(id int, val varchar);
SELECT create_distributed_table('dist_target', 'id');
CREATE TABLE dist_source(id int, val varchar);
SELECT create_distributed_table('dist_source', 'id', colocate_with => 'none');

MERGE INTO dist_target
USING (SELECT 100 id) AS source
ON dist_target.id = source.id AND dist_target.val = 'const'
WHEN MATCHED THEN
UPDATE SET val = 'source'
WHEN NOT MATCHED THEN
INSERT VALUES(source.id, 'source');

-- Non-hash distributed tables (append/range).
CREATE VIEW show_tables AS
SELECT logicalrelid, partmethod
FROM pg_dist_partition
WHERE (logicalrelid = 'dist_target'::regclass) OR (logicalrelid = 'dist_source'::regclass)
ORDER BY 1;

SELECT undistribute_table('dist_source');
SELECT create_distributed_table('dist_source', 'id', 'append');
SELECT * FROM show_tables;

MERGE INTO dist_target
USING dist_source
ON dist_target.id = dist_source.id
WHEN MATCHED THEN
UPDATE SET val = dist_source.val
WHEN NOT MATCHED THEN
INSERT VALUES(dist_source.id, dist_source.val);

SELECT undistribute_table('dist_source');
SELECT create_distributed_table('dist_source', 'id', 'range');
SELECT * FROM show_tables;

MERGE INTO dist_target
USING dist_source
ON dist_target.id = dist_source.id
WHEN MATCHED THEN
UPDATE SET val = dist_source.val
WHEN NOT MATCHED THEN
INSERT VALUES(dist_source.id, dist_source.val);

-- Both are append tables
SELECT undistribute_table('dist_target');
SELECT undistribute_table('dist_source');
SELECT create_distributed_table('dist_target', 'id', 'append');
SELECT create_distributed_table('dist_source', 'id', 'append');
SELECT * FROM show_tables;

MERGE INTO dist_target
USING dist_source
ON dist_target.id = dist_source.id
WHEN MATCHED THEN
UPDATE SET val = dist_source.val
WHEN NOT MATCHED THEN
INSERT VALUES(dist_source.id, dist_source.val);

-- Both are range tables
SELECT undistribute_table('dist_target');
SELECT undistribute_table('dist_source');
SELECT create_distributed_table('dist_target', 'id', 'range');
SELECT create_distributed_table('dist_source', 'id', 'range');
SELECT * FROM show_tables;

MERGE INTO dist_target
USING dist_source
ON dist_target.id = dist_source.id
WHEN MATCHED THEN
UPDATE SET val = dist_source.val
WHEN NOT MATCHED THEN
INSERT VALUES(dist_source.id, dist_source.val);

-- Test Columnar table
CREATE TABLE target_columnar(cid int, name text) USING columnar;
SELECT create_distributed_table('target_columnar', 'cid');
MERGE INTO target_columnar t
USING demo_source_table s
ON t.cid = s.id2
WHEN MATCHED THEN
        UPDATE SET name = 'Columnar table updated by MERGE'
WHEN NOT MATCHED THEN
	DO NOTHING;

MERGE INTO demo_distributed t
USING generate_series(0,100) as source(key)
ON (source.key + 1 = t.id1)
	WHEN MATCHED THEN UPDATE SET val1 = 15;

-- This should fail in planning stage itself
EXPLAIN MERGE INTO demo_distributed t
USING demo_source_table s
ON (s.id2 + 1 = t.id1)
	WHEN MATCHED THEN UPDATE SET val1 = 15;

-- Sub-queries and CTEs are not allowed in actions and ON clause
CREATE TABLE target_1 (a int, b int, c int);
SELECT create_distributed_table('target_1', 'a');

CREATE TABLE source_2 (a int, b int, c int);
SELECT create_distributed_table('source_2', 'a');

INSERT INTO target_1 VALUES(1, 2, 3);
INSERT INTO target_1 VALUES(4, 5, 6);
INSERT INTO target_1 VALUES(11, 12, 13);

INSERT INTO source_2 VALUES(1, 2, 3);

WITH cte_1 as (SELECT max(a) as max_a, max(b) as b FROM source_2)
MERGE INTO target_1
USING cte_1
ON (target_1.a = cte_1.b)
WHEN NOT MATCHED AND (SELECT max_a > 10 FROM cte_1) THEN
	INSERT VALUES (cte_1.b, 100);

WITH cte_1 as (SELECT a, b  FROM source_2)
MERGE INTO target_1
USING cte_1
ON (target_1.a = cte_1.b)
WHEN NOT MATCHED AND (SELECT a > 10 FROM cte_1) THEN
	INSERT VALUES (cte_1.b, 100);

MERGE INTO target_1
USING source_2
ON (target_1.a = source_2.b)
WHEN NOT MATCHED AND (SELECT max_a > 10 FROM (SELECT max(a) as max_a, max(b) as b FROM target_1) as foo) THEN
	INSERT VALUES (source_2.b, 100);

-- or same with CTEs
WITH cte_1 as (SELECT max(a) as max_a, max(b) as b FROM target_1)
MERGE INTO target_1
USING source_2
ON (target_1.a = source_2.b)
WHEN NOT MATCHED AND (SELECT max_a > 10 FROM (SELECT max(a) as max_a, max(b) as b FROM target_1) as foo) THEN
	INSERT VALUES (source_2.b, 100);

WITH cte_1 as (SELECT a, b  FROM target_1), cte_2 as (select b,a from target_1)
MERGE INTO target_1
USING (SELECT * FROM source_2) as subq
ON (target_1.a = subq.b)
WHEN NOT MATCHED AND (SELECT a > 10 FROM cte_2) THEN
	INSERT VALUES (subq.b, 100);

MERGE INTO source_2
USING target_1
ON (target_1.a = source_2.a)
WHEN MATCHED THEN
	UPDATE SET b = (SELECT max(a) FROM source_2);

MERGE INTO source_2
USING target_1
ON (target_1.a = source_2.a)
WHEN NOT MATCHED THEN
	INSERT VALUES (target_1.a,(select max(a) from target_1));

MERGE INTO target_1
USING source_2
ON (target_1.a = source_2.b)
WHEN NOT MATCHED AND (SELECT max(c) > 10 FROM source_2) THEN
	INSERT VALUES (source_2.b, 100);

-- Test in ON clause
MERGE INTO target_1 t2
USING (SELECT * FROM source_2) AS t1
ON (t1.a = t2.a AND (SELECT 1=1 FROM target_1))
WHEN MATCHED THEN
        DELETE;

MERGE INTO target_1 t2
USING (SELECT * FROM source_2) AS t1
ON (t1.a = t2.a AND (SELECT max(a) > 55 FROM target_1))
WHEN MATCHED THEN
        DELETE;

WITH cte_1 as (SELECT a, b  FROM target_1), cte_2 as (select b,a from target_1)
MERGE INTO target_1 t2
USING (SELECT * FROM cte_1) AS t1
ON (t1.a = t2.a AND (SELECT max(a) > 55 FROM cte_2))
WHEN MATCHED THEN
        DELETE;

-- Datatype mismatch between target and source join column
WITH src AS (SELECT FLOOR(b) AS a FROM source_2)
MERGE INTO target_1 t
USING src
ON t.a = src.a
WHEN MATCHED THEN DELETE;

RESET client_min_messages;
DROP SERVER foreign_server CASCADE;
DROP FUNCTION merge_when_and_write();
DROP SCHEMA merge_schema CASCADE;
