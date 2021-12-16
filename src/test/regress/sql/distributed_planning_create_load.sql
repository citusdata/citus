CREATE SCHEMA "distributed planning";
SET search_path TO "distributed planning";

CREATE TABLE
date_part_table (event_time timestamp, event int, user_id int)
partition by range (event_time);

SELECT create_distributed_table('date_part_table', 'user_id');

-- an unnamed index
CREATE INDEX ON date_part_table(user_id, event_time);
--create named and long index with parameters
CREATE INDEX event_idx ON date_part_table(event, event_time, user_id, user_id, event_time);

SELECT create_time_partitions(table_name:='date_part_table',
  partition_interval:= '1 week',
  end_at:= '2020-01-20',
  start_from:='2020-01-01');

INSERT INTO date_part_table
	SELECT '2020-01-01'::timestamp + '3 hours'::interval * i, i, i % 20 FROM generate_series(0,100)i;

CREATE TABLE test(x bigint, y bigint);
SELECT create_distributed_table('test','x');

CREATE TYPE new_type AS (n int, m text);
CREATE TABLE test_2(x bigint, y bigint, z new_type);
SELECT create_distributed_table('test_2','x');

CREATE TABLE ref(a bigint, b bigint);
SELECT create_reference_table('ref');

CREATE TABLE ref2(a bigint, b bigint);
SELECT create_reference_table('ref2');

CREATE TABLE local(c bigint, d bigint);
select citus_add_local_table_to_metadata('local');

CREATE TABLE non_binary_copy_test (key int PRIMARY KEY, value new_type);
SELECT create_distributed_table('non_binary_copy_test', 'key');
INSERT INTO non_binary_copy_test SELECT i, (i, 'citus9.5')::new_type FROM generate_series(0,1000)i;

-- Test upsert with constraint
CREATE TABLE upsert_test
(
	part_key int UNIQUE,
	other_col int,
	third_col int
);

-- distribute the table
SELECT create_distributed_table('upsert_test', 'part_key');

-- do a regular insert
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1), (2, 2) RETURNING *;

create table t1(a int, b int, c int primary key, d int);
ALTER TABLE t1 DROP COLUMN b;
select create_distributed_table('t1','c');
ALTER TABLE t1 DROP COLUMN d;

CREATE TABLE companies (
    id bigint NOT NULL,
    name character varying NOT NULL,
    created_at timestamp without time zone NOT NULL,
    a int,
    b int,
    updated_at timestamp without time zone NOT NULL,
    fmi boolean DEFAULT false NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    deleted_at timestamp without time zone,
    c int,
    flex boolean DEFAULT false
);

ALTER TABLE ONLY companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);

alter table companies drop column a;
alter table companies drop column b;

SELECT create_reference_table('companies');

alter table companies drop column c;
