CREATE SCHEMA test_schema_1;
CREATE SCHEMA IF NOT EXISTS test_schema_2;
CREATE SCHEMA test_schema_3 CREATE TABLE test_table(a INT PRIMARY KEY);

SELECT create_distributed_table('test_schema_3.test_table','a');
INSERT INTO test_schema_3.test_table VALUES (1), (2);

DROP SCHEMA test_schema_2;
CREATE SCHEMA test_schema_4;

ALTER TABLE test_schema_3.test_table SET SCHEMA test_schema_4;
ALTER SCHEMA test_schema_3 RENAME TO test_schema_3_renamed;
ALTER SCHEMA test_schema_4 RENAME TO test_schema_5;
