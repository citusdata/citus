CREATE SCHEMA columnar_test_helpers;
SET search_path TO columnar_test_helpers;

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

CREATE OR REPLACE FUNCTION columnar_store_memory_stats()
    RETURNS TABLE(TopMemoryContext BIGINT,
				  TopTransactionContext BIGINT,
				  WriteStateContext BIGINT)
    LANGUAGE C STRICT VOLATILE
    AS 'citus', $$columnar_store_memory_stats$$;

CREATE FUNCTION top_memory_context_usage()
	RETURNS BIGINT AS $$
		SELECT TopMemoryContext FROM columnar_test_helpers.columnar_store_memory_stats();
	$$ LANGUAGE SQL VOLATILE;
