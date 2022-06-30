--
-- Testing data types without comparison operators
-- If a data type doesn't have comparison operators, we should store NULL for min/max values
-- Verify that (1) min/max entries in columnar.chunk is NULL as expected
-- (2) we can run queries which has equality conditions in WHERE clause for that column with correct results
--

-- varchar
CREATE TABLE test_varchar (a varchar) USING columnar;
INSERT INTO test_varchar VALUES ('Hello');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_varchar WHERE a = 'Hello';
DROP TABLE test_varchar;

-- cidr
CREATE TABLE test_cidr (a cidr) USING columnar;
INSERT INTO test_cidr VALUES ('192.168.100.128/25');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_cidr WHERE a = '192.168.100.128/25';
DROP TABLE test_cidr;

-- json
CREATE TABLE test_json (a json) USING columnar;
INSERT INTO test_json VALUES ('5'::json);
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_json WHERE a::text = '5'::json::text;
DROP TABLE test_json;

-- line
CREATE TABLE test_line (a line) USING columnar;
INSERT INTO test_line VALUES ('{1, 2, 3}');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_line WHERE a = '{1, 2, 3}';
DROP TABLE test_line;

-- lseg
CREATE TABLE test_lseg (a lseg) USING columnar;
INSERT INTO test_lseg VALUES ('( 1 , 2 ) , ( 3 , 4 )');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_lseg WHERE a = '( 1 , 2 ) , ( 3 , 4 )';
DROP TABLE test_lseg;

-- path
CREATE TABLE test_path (a path) USING columnar;
INSERT INTO test_path VALUES ('( 1 , 2 ) , ( 3 , 4 ) , ( 5 , 6 )');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_path WHERE a = '( 1 , 2 ) , ( 3 , 4 ) , ( 5 , 6 )';
DROP TABLE test_path;

-- txid_snapshot
CREATE TABLE test_txid_snapshot (a txid_snapshot) USING columnar;
INSERT INTO test_txid_snapshot VALUES ('10:20:10,14,15');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_txid_snapshot WHERE a::text = '10:20:10,14,15'::txid_snapshot::text;
DROP TABLE test_txid_snapshot;

-- xml
CREATE TABLE test_xml (a xml) USING columnar;
INSERT INTO test_xml VALUES ('<foo>bar</foo>'::xml);
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_xml WHERE a::text = '<foo>bar</foo>'::xml::text;
DROP TABLE test_xml;

-- user defined
CREATE TYPE user_defined_color AS ENUM ('red', 'orange', 'yellow',
                                             'green', 'blue', 'purple');
CREATE TABLE test_user_defined_color (a user_defined_color) USING columnar;
INSERT INTO test_user_defined_color VALUES ('red');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_user_defined_color WHERE a = 'red';
DROP TABLE test_user_defined_color;
DROP TYPE user_defined_color;

-- pg_snapshot
CREATE TABLE test_pg_snapshot (a pg_snapshot) USING columnar;
INSERT INTO test_pg_snapshot VALUES ('10:20:10,14,15');
SELECT minimum_value, maximum_value FROM columnar.chunk;
SELECT * FROM test_pg_snapshot WHERE a::text = '10:20:10,14,15'::pg_snapshot::text;
DROP TABLE test_pg_snapshot;
