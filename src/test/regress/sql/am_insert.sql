--
-- Testing insert on cstore_fdw tables.
--

CREATE TABLE test_insert_command (a int) USING columnar;

-- test single row inserts fail
select count(*) from test_insert_command;
insert into test_insert_command values(1);
select count(*) from test_insert_command;

insert into test_insert_command default values;
select count(*) from test_insert_command;

-- test inserting from another table succeed
CREATE TABLE test_insert_command_data (a int);

select count(*) from test_insert_command_data;
insert into test_insert_command_data values(1);
select count(*) from test_insert_command_data;

insert into test_insert_command select * from test_insert_command_data;
select count(*) from test_insert_command;

SELECT * FROM chunk_group_consistency;

drop table test_insert_command_data;
drop table test_insert_command;

-- test long attribute value insertion
-- create sufficiently long text so that data is stored in toast
CREATE TABLE test_long_text AS
SELECT a as int_val, string_agg(random()::text, '') as text_val
FROM generate_series(1, 10) a, generate_series(1, 1000) b
GROUP BY a ORDER BY a;

-- store hash values of text for later comparison
CREATE TABLE test_long_text_hash AS
SELECT int_val, md5(text_val) AS hash
FROM test_long_text;

CREATE TABLE test_cstore_long_text(int_val int, text_val text)
USING columnar;

-- store long text in cstore table
INSERT INTO test_cstore_long_text SELECT * FROM test_long_text;

SELECT * FROM chunk_group_consistency;

-- drop source table to remove original text from toast
DROP TABLE test_long_text;

-- check if text data is still available in cstore table
-- by comparing previously stored hash.
SELECT a.int_val
FROM  test_long_text_hash a, test_cstore_long_text c
WHERE a.int_val = c.int_val AND a.hash = md5(c.text_val);

DROP TABLE test_long_text_hash;
DROP TABLE test_cstore_long_text;

CREATE TABLE test_logical_replication(i int) USING columnar;
-- should succeed
INSERT INTO test_logical_replication VALUES (1);
CREATE PUBLICATION test_columnar_publication
  FOR TABLE test_logical_replication;
-- should fail; columnar does not support logical replication
INSERT INTO test_logical_replication VALUES (2);
DROP PUBLICATION test_columnar_publication;
-- should succeed
INSERT INTO test_logical_replication VALUES (3);
DROP TABLE test_logical_replication;

--
-- test toast interactions
--

-- row table with data in different storage formats
CREATE TABLE test_toast_row(plain TEXT, main TEXT, external TEXT, extended TEXT);
ALTER TABLE test_toast_row ALTER COLUMN plain SET STORAGE plain; -- inline, uncompressed
ALTER TABLE test_toast_row ALTER COLUMN main SET STORAGE main; -- inline, compressed
ALTER TABLE test_toast_row ALTER COLUMN external SET STORAGE external; -- out-of-line, uncompressed
ALTER TABLE test_toast_row ALTER COLUMN extended SET STORAGE extended; -- out-of-line, compressed

INSERT INTO test_toast_row VALUES(
       repeat('w', 5000), repeat('x', 5000), repeat('y', 5000), repeat('z', 5000));

SELECT
  pg_column_size(plain), pg_column_size(main),
  pg_column_size(external), pg_column_size(extended)
FROM test_toast_row;

CREATE TABLE test_toast_columnar(plain TEXT, main TEXT, external TEXT, extended TEXT)
  USING columnar;
INSERT INTO test_toast_columnar SELECT plain, main, external, extended
  FROM test_toast_row;
SELECT
  pg_column_size(plain), pg_column_size(main),
  pg_column_size(external), pg_column_size(extended)
FROM test_toast_columnar;

SELECT * FROM chunk_group_consistency;

DROP TABLE test_toast_row;
DROP TABLE test_toast_columnar;
