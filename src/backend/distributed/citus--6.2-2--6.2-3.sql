/* citus--6.2-1--6.2-2.sql */

SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION citus_statement_executors(OUT queryid bigint, OUT userid oid, OUT dbid oid, OUT executor bigint, OUT calls bigint)
    RETURNS SETOF record
    LANGUAGE C STRICT COST 1000
    AS 'MODULE_PATHNAME', $$citus_statement_executors$$;

CREATE OR REPLACE FUNCTION citus_statement_executors_reset()
    RETURNS void
    LANGUAGE C STRICT COST 1000
    AS 'MODULE_PATHNAME', $$citus_statement_executors_reset$$;

/*CREATE VIEW pg_dist_statements AS
SELECT CASE d.executor WHEN 1 THEN 'real-time' WHEN 2 THEN 'task-tracker' WHEN 3 THEN 'router' END AS executor,
       s.userid, s.dbid, s.queryid, s.query, s.calls, s.total_time, s.min_time,
       s.max_time, s.mean_time, s.stddev_time, s.rows
  FROM citus_statement_executors() d
  JOIN pg_stat_statements s USING (queryid, userid, dbid);*/

RESET search_path;
