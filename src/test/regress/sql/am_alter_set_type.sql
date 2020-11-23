--
-- Testing ALTER TABLE on columnar tables.
--

CREATE TABLE test_alter_table (a int, b int, c text) USING columnar;

WITH sample_data AS (VALUES
    (1, 2, '3'),
    (4, 5, '6')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

WITH sample_data AS (VALUES
    (5, 9, '11'),
    (12, 83, '93')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

ALTER TABLE test_alter_table ALTER COLUMN a TYPE jsonb USING row_to_json(row(a));
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN c TYPE int USING c::integer;
SELECT sum(c) FROM test_alter_table;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE bigint;
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE float USING (b::float + 0.5);
SELECT * FROM test_alter_table ORDER BY a;

DROP TABLE test_alter_table;
