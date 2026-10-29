CREATE SCHEMA nested_execution;
SET search_path TO nested_execution;
GRANT ALL ON SCHEMA nested_execution TO regularuser;

-- some of the next_execution tests change for single shard
SET citus.shard_count TO 4;

CREATE TABLE distributed (key int, name text,
                    	  created_at timestamptz DEFAULT now());
CREATE TABLE reference (id bigint PRIMARY KEY, title text);

SELECT create_distributed_table('distributed', 'key');
SELECT create_reference_table('reference');

INSERT INTO distributed SELECT i,  i::text, now() FROM generate_series(1,10)i;
INSERT INTO reference SELECT i,  i::text FROM generate_series(1,10)i;

CREATE FUNCTION dist_query_single_shard(p_key int)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
	result bigint;
BEGIN
    SELECT count(*) INTO result FROM nested_execution.distributed WHERE key = p_key;
	RETURN result;
END;
$$;

CREATE FUNCTION dist_query_multi_shard()
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
	result bigint;
BEGIN
    SELECT count(*) INTO result FROM nested_execution.distributed;
	RETURN result;
END;
$$;

CREATE FUNCTION ref_query()
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
	result bigint;
BEGIN
    SELECT count(*) INTO result FROM nested_execution.reference;
	RETURN result;
END;
$$;
