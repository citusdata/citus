--
-- Test the CREATE statements related to columnar.
--


-- Create uncompressed table
CREATE TABLE contestant (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- should fail
CREATE INDEX contestant_idx on contestant(handle);

-- Create compressed table with automatically determined file path
-- COMPRESSED
CREATE TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- Test that querying an empty table works
ANALYZE contestant;
SELECT count(*) FROM contestant;

-- Should fail: unlogged tables not supported
CREATE UNLOGGED TABLE columnar_unlogged(i int) USING columnar;

-- Should fail: temporary tables not supported
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

--
-- Utility functions to be used throughout tests
--

CREATE FUNCTION columnar_relation_storageid(relid oid) RETURNS bigint
    LANGUAGE C STABLE STRICT
    AS 'citus', $$columnar_relation_storageid$$;

CREATE FUNCTION compression_type_supported(type text) RETURNS boolean
AS $$
BEGIN
   EXECUTE 'SET LOCAL columnar.compression TO ' || quote_literal(type);
   return true;
EXCEPTION WHEN invalid_parameter_value THEN
   return false;
END;
$$ LANGUAGE plpgsql;


-- are chunk groups and chunks consistent?
CREATE view chunk_group_consistency AS
WITH a as (
   SELECT storage_id, stripe_num, chunk_group_num, min(value_count) as row_count
   FROM columnar.chunk
   GROUP BY 1,2,3
), b as (
   SELECT storage_id, stripe_num, chunk_group_num, max(value_count) as row_count
   FROM columnar.chunk
   GROUP BY 1,2,3
), c as (
   (TABLE a EXCEPT TABLE b) UNION (TABLE b EXCEPT TABLE a) UNION
   (TABLE a EXCEPT TABLE columnar.chunk_group) UNION (TABLE columnar.chunk_group EXCEPT TABLE a)
), d as (
   SELECT storage_id, stripe_num, count(*) as chunk_group_count
   FROM columnar.chunk_group
   GROUP BY 1,2
), e as (
   SELECT storage_id, stripe_num, chunk_group_count
   FROM columnar.stripe
), f as (
   (TABLE d EXCEPT TABLE d) UNION (TABLE e EXCEPT TABLE d)
)
SELECT (SELECT count(*) = 0 FROM c) AND
       (SELECT count(*) = 0 FROM f) as consistent;
