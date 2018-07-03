/* citus--7.5-4--7.5-5 */
CREATE FUNCTION pg_catalog.citus_executor_name(executor_type int)
RETURNS TEXT
LANGUAGE plpgsql
AS $function$
BEGIN
	IF (executor_type = 1) THEN
		RETURN 'real-time';
	ELSIF (executor_type = 2) THEN
		RETURN 'task-tracker';
	ELSIF (executor_type = 3) THEN
		RETURN 'router';
	ELSIF (executor_type = 4) THEN
		RETURN 'insert-select';
	ELSE
		RETURN 'unknown';
	END IF;
END;
$function$;

DROP VIEW pg_catalog.citus_stat_statements;

CREATE VIEW citus.citus_stat_statements AS
SELECT
  queryid,
  userid,
  dbid,
  query,
  pg_catalog.citus_executor_name(executor::int) AS executor,
  partition_key,
  calls
FROM pg_catalog.citus_stat_statements();
ALTER VIEW citus.citus_stat_statements SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stat_statements TO public;
