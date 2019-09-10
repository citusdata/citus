/* citus--8.0-11--8.0-12 */
SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_statements(OUT queryid bigint,
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
 				 	USING (queryid, userid, dbid);
 ELSE
    RAISE EXCEPTION 'pg_stat_statements is not installed'
    	USING HINT = 'install pg_stat_statements extension and try again';
 END IF;
END;
$citus_stat_statements$;

RESET search_path;
