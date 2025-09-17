CREATE SCHEMA columnar_chunk_test;
SET search_path TO columnar_chunk_test;
SET columnar.compression TO 'none';

-- set to debug1 to see how many new chunks has been created during
-- chunk_group_size_limit overflow

SET client_min_messages TO debug1;


--
-- ISSUE_6420
--
-- Issue: Automatically allocate a new chunk group instead of throwing error due to buffer size limits
-- Link: https://github.com/citusdata/citus/issues/6420
--
-- Insert rows that exceeds the chunk group size limit.
-- Adding 600 rows each with the size of 2MB will eventually exceeds the
-- limit of 1GB for enlargeStringInfo() but this should not fail.
-- Also setting chunk_group_size_limit to will exceed the max chunk groups limit 5000/1000 = 5, new
-- chunkgroup should be allocated automatically

CREATE TABLE test_oversized_row (
    id INTEGER,
    huge_text TEXT
) USING columnar WITH (
    columnar.chunk_group_row_limit = 1000,
    columnar.stripe_row_limit = 5000,
    columnar.chunk_group_size_limit = 128
);

INSERT INTO test_oversized_row
SELECT gs, repeat('Y', 2*1024*1024)  -- 2 MB text
FROM generate_series(1, 600) AS gs;

SELECT * FROM columnar.chunk_group WHERE relation = 'test_oversized_row'::regclass;
SELECT * FROM columnar.stripe WHERE relation = 'test_oversized_row'::regclass;

-- test edge case setting chunk_group_size_limit = 1024
DROP TABLE test_oversized_row;

CREATE TABLE test_oversized_row (
    id INTEGER,
    huge_text TEXT
) USING columnar WITH (
    columnar.chunk_group_row_limit = 1000,
    columnar.stripe_row_limit = 5000,
    columnar.chunk_group_size_limit = 1024
);

INSERT INTO test_oversized_row
SELECT gs, repeat('Y', 2*1024*1024)  -- 2 MB text
FROM generate_series(1, 600) AS gs;

SELECT * FROM columnar.chunk_group WHERE relation = 'test_oversized_row'::regclass;
SELECT * FROM columnar.stripe WHERE relation = 'test_oversized_row'::regclass;

-- test VACUUM FULL
VACUUM FULL test_oversized_row;

SET client_min_messages TO warning;

-- try verifying the data integrity
SELECT COUNT(*) FROM test_oversized_row;
SELECT ID, LENGTH(huge_text) FROM test_oversized_row ORDER BY id LIMIT 10;

-- total size should be greater 1GB (1258291200 bytes)
SELECT SUM(LENGTH(huge_text)) AS total_size FROM test_oversized_row;

\dt+ test_oversized_row

DROP TABLE test_oversized_row;
DROP SCHEMA columnar_chunk_test CASCADE;