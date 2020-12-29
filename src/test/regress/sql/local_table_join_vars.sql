CREATE SCHEMA local_table_join_vars;
SET search_path TO local_table_join_vars;

SET client_min_messages to ERROR;
SELECT master_add_node('localhost', :master_port, groupId => 0) AS coordinator_nodeid \gset

SET client_min_messages TO DEBUG1;

CREATE TABLE postgres_table (key int, value text, value_2 jsonb);
CREATE TABLE reference_table (key int, value text, value_2 jsonb);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table (key int, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table', 'key');

CREATE TABLE distributed_table_pkey (key int primary key, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_pkey', 'key');

CREATE TABLE distributed_table_windex (key int primary key, value text, value_2 jsonb);
SELECT create_distributed_table('distributed_table_windex', 'key');
CREATE UNIQUE INDEX key_index ON distributed_table_windex (key);

CREATE TABLE citus_local(key int, value text);
SELECT create_citus_local_table('citus_local');

CREATE TABLE distributed_partitioned_table(key int, value text) PARTITION BY RANGE (key);
CREATE TABLE distributed_partitioned_table_1 PARTITION OF distributed_partitioned_table FOR VALUES FROM (0) TO (50);
CREATE TABLE distributed_partitioned_table_2 PARTITION OF distributed_partitioned_table FOR VALUES FROM (50) TO (200);
SELECT create_distributed_table('distributed_partitioned_table', 'key');

CREATE TABLE local_partitioned_table(key int, value text) PARTITION BY RANGE (key);
CREATE TABLE local_partitioned_table_1 PARTITION OF local_partitioned_table FOR VALUES FROM (0) TO (50);
CREATE TABLE local_partitioned_table_2 PARTITION OF local_partitioned_table FOR VALUES FROM (50) TO (200);

CREATE TABLE distributed_table_composite (key int, value text, value_2 jsonb, primary key (key, value));
SELECT create_distributed_table('distributed_table_composite', 'key');

INSERT INTO postgres_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO reference_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_windex SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_pkey SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO distributed_table_composite SELECT i, i::varchar(256) FROM generate_series(1, 100) i;
INSERT INTO local_partitioned_table SELECT i, i::varchar(256) FROM generate_series(1, 100) i;

-- vars referencing outer queries should work in SELECT
SELECT (SELECT COUNT(*) FROM postgres_table WHERE postgres_table.key = distributed_table.key) FROM distributed_table ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM postgres_table WHERE distributed_table.key = 5) FROM distributed_table ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM distributed_table WHERE postgres_table.key = 5) FROM postgres_table ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM distributed_table WHERE postgres_table.key = distributed_table.key) FROM postgres_table ORDER BY 1 LIMIT 1;

-- vars referencing outer queries should work in SELECT with citus local tables
SELECT (SELECT COUNT(*) FROM citus_local WHERE citus_local.key = distributed_table.key) FROM distributed_table ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM citus_local WHERE distributed_table.key = 5) FROM distributed_table ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM distributed_table WHERE citus_local.key = 5) FROM citus_local ORDER BY 1 LIMIT 1;
SELECT (SELECT COUNT(*) FROM distributed_table WHERE citus_local.key = distributed_table.key) FROM citus_local ORDER BY 1 LIMIT 1;

-- vars referencing outer queries should work in WHERE
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM postgres_table WHERE key > distributed_table.key);
SELECT COUNT(*) FROM postgres_table WHERE key in (SELECT key FROM distributed_table WHERE key > postgres_table.key);
SELECT COUNT(*) FROM postgres_table WHERE key in (SELECT key FROM distributed_table WHERE postgres_table.key > 5);
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM postgres_table WHERE distributed_table.key > 5);

-- vars referencing outer queries should work in WHERE with citus local tables
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM citus_local WHERE key > distributed_table.key);
SELECT COUNT(*) FROM citus_local WHERE key in (SELECT key FROM distributed_table WHERE key > citus_local.key);
SELECT COUNT(*) FROM citus_local WHERE key in (SELECT key FROM distributed_table WHERE citus_local.key > 5);
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM citus_local WHERE distributed_table.key > 5);

-- citus-local, local and dist should work
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM citus_local JOIN postgres_table USING(key) WHERE key > distributed_table.key);
-- Will work when we fix "correlated subqueries are not supported when the FROM clause contains a CTE or subquery"
SELECT COUNT(*) FROM citus_local WHERE key in (SELECT key FROM distributed_table JOIN postgres_table USING(key) WHERE key > citus_local.key);
-- Will work when we fix "correlated subqueries are not supported when the FROM clause contains a CTE or subquery"
SELECT COUNT(*) FROM citus_local WHERE key in (SELECT key FROM distributed_table JOIN postgres_table USING(key) WHERE citus_local.key > 5);
SELECT COUNT(*) FROM distributed_table WHERE key in (SELECT key FROM citus_local JOIN postgres_table USING(key) WHERE distributed_table.key > 5);

-- Will work when we fix "correlated subqueries are not supported when the FROM clause contains a CTE or subquery"
SELECT (SELECT (SELECT COUNT(*) FROM postgres_table WHERE postgres_table.key = distributed_table.key) FROM postgres_table p2) FROM distributed_table;


RESET client_min_messages;
\set VERBOSITY terse
DROP SCHEMA local_table_join_vars CASCADE;
