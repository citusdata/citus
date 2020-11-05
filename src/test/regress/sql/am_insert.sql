--
-- Testing insert on cstore_fdw tables.
--

CREATE TABLE test_insert_command (a int) USING cstore_tableam;

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
USING cstore_tableam;

-- store long text in cstore table
INSERT INTO test_cstore_long_text SELECT * FROM test_long_text;

-- drop source table to remove original text from toast
DROP TABLE test_long_text;

-- check if text data is still available in cstore table
-- by comparing previously stored hash.
SELECT a.int_val
FROM  test_long_text_hash a, test_cstore_long_text c
WHERE a.int_val = c.int_val AND a.hash = md5(c.text_val);

DROP TABLE test_long_text_hash;
DROP TABLE test_cstore_long_text;
