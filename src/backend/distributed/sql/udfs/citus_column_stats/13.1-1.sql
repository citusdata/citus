CREATE OR REPLACE FUNCTION pg_catalog.citus_column_stats(
    qualified_table_name text)
RETURNS TABLE (
    attname text,
    most_common_vals text[],
    most_common_freqs float4[]
)
AS $func$
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_dist_partition WHERE logicalrelid = qualified_table_name::regclass) THEN
        RAISE EXCEPTION 'Not a Citus table';
    ELSE
        RETURN QUERY

        WITH most_common_vals_json AS (
        SELECT * FROM run_command_on_shards(qualified_table_name,
                                            $$ SELECT json_agg(row_to_json(shard_stats)) FROM (
                                            SELECT attname, most_common_vals, most_common_freqs, c.reltuples AS reltuples
                                            FROM pg_stats s RIGHT JOIN pg_class c ON (s.tablename = c.relname)
                                            WHERE c.relname = '%s') shard_stats $$ )),

        table_reltuples_json AS (
        SELECT distinct(shardid),
               (json_array_elements(result::json)->>'reltuples')::bigint AS shard_reltuples
               FROM most_common_vals_json),

        table_reltuples AS (
        SELECT sum(shard_reltuples) AS table_reltuples FROM table_reltuples_json),

        most_common_vals AS (
        SELECT shardid,
               (json_array_elements(result::json)->>'attname')::text AS attname,
               json_array_elements_text((json_array_elements(result::json)->>'most_common_vals')::json)::text AS common_val,
               json_array_elements_text((json_array_elements(result::json)->>'most_common_freqs')::json)::float4 AS common_freq,
               (json_array_elements(result::json)->>'reltuples')::bigint AS shard_reltuples
        FROM most_common_vals_json),

        common_val_occurrence AS (
        SELECT m.attname, common_val, sum(common_freq * shard_reltuples)::bigint AS occurrence
        FROM most_common_vals m
        GROUP BY m.attname, common_val
        ORDER BY m.attname, occurrence DESC, common_val)

        SELECT c.attname,
               ARRAY_agg(common_val) AS most_common_vals,
               ARRAY_agg((occurrence/t.table_reltuples)::float4) AS most_common_freqs
        FROM common_val_occurrence c, table_reltuples t
        GROUP BY c.attname;

    END IF;
END;
$func$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pg_catalog.citus_column_stats(
    qualified_table_name text)
    IS 'provides some pg_stats for columns of input Citus table';
