--
-- MULTI_INSERT_QUERY
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1300000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1300000;

CREATE SCHEMA insert_query
CREATE TABLE hash_table (
    key int,
    value text,
    attributes text[]
)
CREATE TABLE append_table (
    LIKE hash_table
)
CREATE TABLE staging_table (
    LIKE hash_table
);

SELECT master_create_distributed_table('insert_query.hash_table', 'key', 'hash');
SELECT master_create_worker_shards('insert_query.hash_table', 4, 2);

SELECT master_create_distributed_table('insert_query.append_table', 'key', 'append');

-- Try to insert into both distributed tables from staging table
SELECT master_insert_query_result('insert_query.hash_table', $$
  SELECT s, 'value-'||s, ARRAY['a-'||s,'b-'||s] FROM generate_series(1, 1000) s
$$);

SELECT count(*) FROM insert_query.hash_table;
SELECT * FROM insert_query.hash_table LIMIT 1;

-- Try to insert into both distributed tables from staging table
SELECT master_insert_query_result('insert_query.append_table', $$
  SELECT s, 'value-'||s, ARRAY['a-'||s,'b-'||s] FROM generate_series(1001, 2000) s
$$);

SELECT count(*) FROM insert_query.append_table;
SELECT * FROM insert_query.append_table LIMIT 1;

-- Load 1000 rows in to staging table
INSERT INTO insert_query.staging_table
SELECT s, 'value-'||s, ARRAY['a-'||s,'b-'||s] FROM generate_series(2001, 3000) s;

-- Move all the rows into target table
SELECT master_insert_query_result('insert_query.hash_table',
                                  'DELETE FROM insert_query.staging_table RETURNING *');

SELECT count(*) FROM insert_query.hash_table;

-- Copy from a distributed table to a distributed table
SELECT master_insert_query_result('insert_query.append_table', 
                                  'SELECT * FROM insert_query.hash_table LIMIT 10');

SELECT count(*) FROM insert_query.append_table;

-- Too many columns
SELECT master_insert_query_result('insert_query.hash_table', $$
  SELECT key, value, attributes, attributes FROM insert_query.append_table
$$);

-- Too few columns
SELECT master_insert_query_result('insert_query.hash_table', $$
  SELECT key, value FROM insert_query.append_table
$$);

-- Non-matching data type
SELECT master_insert_query_result('insert_query.hash_table', $$
  SELECT key, attributes, value FROM insert_query.append_table
$$);
