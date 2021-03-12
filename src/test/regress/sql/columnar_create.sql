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
   (TABLE d EXCEPT TABLE e) UNION (TABLE e EXCEPT TABLE d)
)
SELECT (SELECT count(*) = 0 FROM c) AND
       (SELECT count(*) = 0 FROM f) as consistent;

CREATE FUNCTION columnar_metadata_has_storage_id(input_storage_id bigint) RETURNS boolean
AS $$
DECLARE
   union_storage_id_count integer;
BEGIN
   SELECT count(*) INTO union_storage_id_count FROM
   (
   SELECT storage_id FROM columnar.stripe UNION ALL
   SELECT storage_id FROM columnar.chunk UNION ALL
   SELECT storage_id FROM columnar.chunk_group
   ) AS union_storage_id
   WHERE storage_id=input_storage_id;

   IF union_storage_id_count > 0 THEN
   RETURN true;
   END IF;

   RETURN false;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE columnar_table_1 (a int) USING columnar;
INSERT INTO columnar_table_1 VALUES (1);

CREATE MATERIALIZED VIEW columnar_table_1_mv USING columnar
AS SELECT * FROM columnar_table_1;

SELECT columnar_relation_storageid(oid) AS columnar_table_1_mv_storage_id
FROM pg_class WHERE relname='columnar_table_1_mv' \gset

-- test columnar_relation_set_new_filenode
REFRESH MATERIALIZED VIEW columnar_table_1_mv;
SELECT columnar_metadata_has_storage_id(:columnar_table_1_mv_storage_id);

SELECT columnar_relation_storageid(oid) AS columnar_table_1_storage_id
FROM pg_class WHERE relname='columnar_table_1' \gset

BEGIN;
  -- test columnar_relation_nontransactional_truncate
  TRUNCATE columnar_table_1;
  SELECT columnar_metadata_has_storage_id(:columnar_table_1_storage_id);
ROLLBACK;

-- since we rollback'ed above xact, should return true
SELECT columnar_metadata_has_storage_id(:columnar_table_1_storage_id);

-- test dropping columnar table
DROP TABLE columnar_table_1 CASCADE;
SELECT columnar_metadata_has_storage_id(:columnar_table_1_storage_id);
