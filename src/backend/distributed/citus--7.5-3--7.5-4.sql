/* citus--7.5-3--7.5-4 */

CREATE FUNCTION pg_catalog.citus_query_stats(OUT queryid bigint,
											 OUT userid oid,
											 OUT dbid oid,
											 OUT executor bigint,
											 OUT partition_key text,
											 OUT calls bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_query_stats$$;

CREATE FUNCTION pg_catalog.citus_stat_statements_reset()
RETURNS VOID
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_stat_statements_reset$$;

CREATE FUNCTION pg_catalog.citus_stat_statements(OUT queryid bigint,
												 OUT userid oid,
												 OUT dbid oid,
												 OUT query text,
												 OUT executor bigint,
												 OUT partition_key text,
												 OUT calls bigint)
RETURNS SETOF record
LANGUAGE plpgsql
AS $citus_stat_statements$
BEGIN
 IF EXISTS (
 	SELECT extname FROM pg_extension
 	WHERE extname = 'pg_stat_statements')
 THEN
 	RETURN QUERY SELECT pss.queryid, pss.userid, pss.dbid, pss.query, cqs.executor,
 						cqs.partition_key, cqs.calls
 				 FROM pg_stat_statements(true) pss
 				 	JOIN citus_query_stats() cqs
 				 	USING (queryid);
 ELSE
    RAISE EXCEPTION 'pg_stat_statements is not installed'
    	USING HINT = 'install pg_stat_statements extension and try again';
 END IF;
END;
$citus_stat_statements$;

CREATE VIEW citus.citus_stat_statements as SELECT * FROM pg_catalog.citus_stat_statements();
ALTER VIEW citus.citus_stat_statements SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stat_statements TO public;
