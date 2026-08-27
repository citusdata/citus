CREATE SCHEMA extract_deparse;
SET search_path TO extract_deparse;

-- EXTRACT accepts any string as its field name. Verify Citus quotes that field
-- when deparsing task SQL, so text resembling a second statement remains data.
CREATE TABLE extract_deparse_source (id int, ts timestamp);
SELECT create_distributed_table('extract_deparse_source', 'id',
								shard_count => 1, colocate_with => 'none');
INSERT INTO extract_deparse_source VALUES (1, timestamp '2026-01-01');

DO $$
BEGIN
	PERFORM EXTRACT('year FROM timestamp ''2000-01-01''); CREATE TABLE injected(); --' FROM ts)
	FROM extract_deparse_source
	WHERE id = 1;
EXCEPTION
	WHEN invalid_parameter_value THEN NULL;
END
$$;

SELECT bool_and(result::boolean) AS extract_field_injection_blocked
FROM run_command_on_workers($$
	SELECT to_regclass('extract_deparse.injected') IS NULL
$$);

DROP SCHEMA extract_deparse CASCADE;
