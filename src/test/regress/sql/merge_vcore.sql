-- MERGE command performs a join from data_source to target_table_name
DROP SCHEMA IF EXISTS merge_vcore_schema CASCADE;
--MERGE INTO target
--USING source
--WHEN NOT MATCHED
--WHEN MATCHED AND <condition>
--WHEN MATCHED

CREATE SCHEMA merge_vcore_schema;
SET search_path TO merge_vcore_schema;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 4000000;
SET citus.explain_all_tasks TO true;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SET client_min_messages = warning;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;


-- ****************************************** CASE 1 : Both are singleSharded***************************************
CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', null, colocate_with=>'none');
SELECT create_distributed_table('target', null, colocate_with=>'none');

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;


DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- *************** CASE 2 : source is single sharded and target is distributed *******************************
CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', null, colocate_with=>'none');
SELECT create_distributed_table('target', 'id');

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;



DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- *************** CASE 3 : source is distributed  and target is single sharded *******************************
CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', 'id');
SELECT create_distributed_table('target', null);

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- *************** CASE 4 : both are distributed  *******************************
CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', 'id');
SELECT create_distributed_table('target', 'id');

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- *************** CASE 5 : both are distributed & colocated *******************************

CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', 'id');
SELECT create_distributed_table('target', 'id', colocate_with=>'source');

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- *************** CASE 6 : both are singlesharded & colocated *******************************

CREATE TABLE source (
    id bigint,
    doc text
);

CREATE TABLE target (
    id bigint,
    doc text
);

SELECT create_distributed_table('source', null);
SELECT create_distributed_table('target', null, colocate_with=>'source');

INSERT INTO source (id, doc) VALUES (1, '{"a" : 1}'), (1, '{"a" : 2}');

-- insert
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- update
MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id) AND src.doc = target.doc
WHEN MATCHED THEN
UPDATE SET doc = '{"b" : 1}'
WHEN NOT MATCHED THEN
INSERT (id, doc)
VALUES (src.t_id, doc);

SELECT * FROM target;

-- Explain
EXPLAIN (costs off, timing off, summary off) MERGE INTO ONLY target USING (SELECT 2::bigint AS t_id, doc FROM source) src
ON (src.t_id = target.id)
WHEN MATCHED THEN DO NOTHING;

DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- Bug Fix Test as part of this PR
-- Test 1
CREATE TABLE source (
    id int,
    age int,
    salary int
);

CREATE TABLE target (
    id int,
    age int,
    salary int
);

SELECT create_distributed_table('source', 'id', colocate_with=>'none');
SELECT create_distributed_table('target', 'id', colocate_with=>'none');

INSERT INTO source (id, age, salary) VALUES  (1,30, 100000);

MERGE INTO ONLY target USING source ON (source.id = target.id)
WHEN NOT MATCHED THEN
INSERT (id, salary) VALUES (source.id, source.salary);

SELECT * FROM TARGET;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- Test 2
CREATE TABLE source (
    id int,
    age int,
    salary int
);

CREATE TABLE target (
    id int,
    age int,
    salary int
);

SELECT create_distributed_table('source', 'id', colocate_with=>'none');
SELECT create_distributed_table('target', 'id', colocate_with=>'none');

INSERT INTO source (id, age, salary) VALUES  (1,30, 100000);

MERGE INTO ONLY target USING source ON (source.id = target.id)
WHEN NOT MATCHED THEN
INSERT (salary, id) VALUES (source.salary, source.id);

SELECT * FROM TARGET;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;


-- Test 3
CREATE TABLE source (
    id int,
    age int,
    salary int
);

CREATE TABLE target (
    id int,
    age int,
    salary int
);

SELECT create_distributed_table('source', 'id', colocate_with=>'none');
SELECT create_distributed_table('target', 'id', colocate_with=>'none');

INSERT INTO source (id, age, salary) VALUES  (1,30, 100000);

MERGE INTO ONLY target USING source ON (source.id = target.id)
WHEN NOT MATCHED THEN
INSERT (salary, id, age) VALUES (source.age, source.id, source.salary);

SELECT * FROM TARGET;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS target;



DROP SCHEMA IF EXISTS merge_vcore_schema CASCADE;




