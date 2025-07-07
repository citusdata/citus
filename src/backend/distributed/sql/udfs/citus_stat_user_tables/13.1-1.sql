CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_user_tables(
RETURNS TABLE (
    dist_table regclass,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_tup_newpage_upd bigint,
    n_live_tup bigint,
    n_dead_tup bigint
)
AS $func$
BEGIN
    RETURN QUERY

    WITH pg_dist_stats_double_json AS (
        SELECT ( SELECT json_agg(row_to_json(f)) FROM ( SELECT result FROM
            run_command_on_shards(logicalrelid, $$ SELECT json_agg(row_to_json(d))
                FROM ( SELECT '$$ || logicalrelid || $$' AS dist_table,
			            s.relname, n_tup_ins, n_tup_upd, n_tup_del,
                        n_tup_hot_upd, n_tup_newpage_upd, n_live_tup, n_dead_tup
                        FROM pg_stat_user_tables s
                        JOIN pg_class c ON s.relname = c.relname
                        WHERE c.oid = '%s'::regclass::oid) d $$)) f)
            FROM pg_dist_partition),

    pg_dist_stats_single_json AS (
        SELECT (json_array_elements(json_agg)->>'result') AS result
        FROM pg_dist_stats_double_json),

    pg_dist_stats_regular AS (
        SELECT (json_array_elements(result::json)->>'dist_table')::regclass AS dist_table,
               (json_array_elements(result::json)->>'relname')::name AS shardname,
               (json_array_elements(result::json)->>'n_tup_ins')::bigint AS n_tup_ins,
               (json_array_elements(result::json)->>'n_tup_upd')::bigint AS n_tup_upd,
               (json_array_elements(result::json)->>'n_tup_del')::bigint AS n_tup_del,
               (json_array_elements(result::json)->>'n_tup_hot_upd')::bigint AS n_tup_hot_upd,
               (json_array_elements(result::json)->>'n_tup_newpage_upd')::bigint AS n_tup_newpage_upd,
               (json_array_elements(result::json)->>'n_live_tup')::bigint AS n_live_tup,
               (json_array_elements(result::json)->>'n_dead_tup')::bigint AS n_dead_tup
        FROM pg_dist_stats_single_json
        WHERE result != '')

    SELECT dist_table, sum(n_tup_ins)::bigint AS n_tup_ins, sum(n_tup_upd)::bigint AS n_tup_upd,
           sum(n_tup_del)::bigint AS n_tup_del, sum(n_tup_hot_upd)::bigint AS n_tup_hot_upd,
           sum(n_tup_newpage_upd)::bigint AS n_tup_newpage_upd,
           sum(n_live_tup)::bigint AS n_live_tup, sum(n_dead_tup)::bigint AS n_dead_tup
    FROM pg_dist_stats_regular
    GROUP BY 1 ORDER BY 1;

END;
$func$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pg_catalog.citus_stat_user_tables(
    qualified_table_name text)
    IS 'provides some pg_stat_user_tables entries for Citus tables';
