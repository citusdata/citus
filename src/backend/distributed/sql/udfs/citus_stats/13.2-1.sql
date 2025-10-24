SET search_path = 'pg_catalog';
DROP VIEW IF EXISTS pg_catalog.citus_stats;

CREATE OR REPLACE VIEW citus.citus_stats AS

WITH most_common_vals_double_json AS (
    SELECT ( SELECT json_agg(row_to_json(f)) FROM ( SELECT * FROM run_command_on_shards(logicalrelid,
            $$ SELECT json_agg(row_to_json(shard_stats)) FROM (
            SELECT '$$ || logicalrelid || $$' AS citus_table, attname, s.null_frac,
                   most_common_vals, most_common_freqs, c.reltuples AS reltuples
            -- join on tablename is enough here, no need to join with pg_namespace
            -- since shards have unique ids in their names, hence two shard names
            -- could never be the same
            FROM pg_stats s RIGHT JOIN pg_class c ON (s.tablename = c.relname)
            WHERE c.oid = '%s'::regclass) shard_stats $$ ))f)
        FROM pg_dist_partition),

most_common_vals_json AS (
    SELECT (json_array_elements(json_agg)->>'result') AS result,
	       (json_array_elements(json_agg)->>'shardid') AS shardid
    FROM most_common_vals_double_json),

table_reltuples_json AS (
    SELECT distinct(shardid),
           CAST( CAST((json_array_elements(result::json)->>'reltuples') AS DOUBLE PRECISION) AS bigint) AS shard_reltuples,
	       (json_array_elements(result::json)->>'citus_table')::regclass AS citus_table
    FROM most_common_vals_json),

table_reltuples AS (
        SELECT citus_table, sum(shard_reltuples) AS table_reltuples
        FROM table_reltuples_json GROUP BY 1 ORDER BY 1),

null_frac_json AS (
    SELECT (json_array_elements(result::json)->>'citus_table')::regclass AS citus_table,
           CAST( CAST((json_array_elements(result::json)->>'reltuples') AS DOUBLE PRECISION) AS bigint) AS shard_reltuples,
           CAST((json_array_elements(result::json)->>'null_frac') AS float4) AS null_frac,
           (json_array_elements(result::json)->>'attname')::text AS attname
    FROM most_common_vals_json
),

null_occurrences AS (
    SELECT citus_table, attname, sum(null_frac * shard_reltuples)::bigint AS null_occurrences
    FROM null_frac_json
    GROUP BY 1, 2
    ORDER BY 1, 2
),

most_common_vals AS (
    SELECT (json_array_elements(result::json)->>'citus_table')::regclass AS citus_table,
           (json_array_elements(result::json)->>'attname')::text AS attname,
           json_array_elements_text((json_array_elements(result::json)->>'most_common_vals')::json)::text AS common_val,
           CAST(json_array_elements_text((json_array_elements(result::json)->>'most_common_freqs')::json) AS float4) AS common_freq,
           CAST( CAST((json_array_elements(result::json)->>'reltuples') AS DOUBLE PRECISION) AS bigint) AS shard_reltuples
    FROM most_common_vals_json),

common_val_occurrence AS (
    SELECT citus_table, m.attname, common_val,
            sum(common_freq * shard_reltuples)::bigint AS occurrence
    FROM most_common_vals m
    GROUP BY citus_table, m.attname, common_val
    ORDER BY 1, 2, occurrence DESC, 3)

SELECT nsp.nspname AS schemaname, p.relname AS tablename, c.attname,

       CASE WHEN max(t.table_reltuples::bigint) = 0 THEN 0
       ELSE max(n.null_occurrences/t.table_reltuples)::float4 END AS null_frac,

       ARRAY_agg(common_val) AS most_common_vals,

       CASE WHEN max(t.table_reltuples::bigint) = 0 THEN NULL
       ELSE ARRAY_agg((occurrence/t.table_reltuples)::float4) END AS most_common_freqs

FROM common_val_occurrence c, table_reltuples t, null_occurrences n, pg_class p, pg_namespace nsp
WHERE c.citus_table = t.citus_table
      AND c.citus_table = n.citus_table AND c.attname = n.attname
      AND c.citus_table::regclass::oid = p.oid AND p.relnamespace = nsp.oid
GROUP BY nsp.nspname, c.citus_table, p.relname, c.attname;

ALTER VIEW citus.citus_stats SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stats TO PUBLIC;

RESET search_path;
