CREATE SCHEMA sequences_schema;
SET search_path TO sequences_schema;

CREATE SEQUENCE seq_0;
CREATE SEQUENCE seq_0_local_table;
ALTER SEQUENCE seq_0 AS smallint;

CREATE TABLE seq_test_0 (x int, y int);
CREATE TABLE seq_test_0_local_table (x int, y int);
SELECT create_distributed_table('seq_test_0','x');

INSERT INTO seq_test_0 SELECT 1, s FROM generate_series(1, 50) s;
INSERT INTO seq_test_0_local_table SELECT 1, s FROM generate_series(1, 50) s;
ALTER TABLE seq_test_0 ADD COLUMN z int DEFAULT nextval('seq_0');
-- follow hint
ALTER TABLE seq_test_0 ADD COLUMN z int;
ALTER TABLE seq_test_0 ALTER COLUMN z SET DEFAULT nextval('seq_0');
SELECT * FROM seq_test_0 ORDER BY 1, 2 LIMIT 5;

ALTER TABLE seq_test_0_local_table ADD COLUMN z int;
ALTER TABLE seq_test_0_local_table ALTER COLUMN z SET DEFAULT nextval('seq_0_local_table');
SELECT * FROM seq_test_0_local_table ORDER BY 1, 2 LIMIT 5;

-- cannot alter a sequence used in a distributed table
-- since the metadata is synced to workers
ALTER SEQUENCE seq_0 AS bigint;

DROP SCHEMA sequences_schema CASCADE;
