-- cts in the query is an abbreviation for citus_stats_tenants
CREATE OR REPLACE FUNCTION pg_catalog.citus_stats_tenants (
    return_all_tenants BOOLEAN DEFAULT FALSE,
    OUT nodeid INT,
    OUT colocation_id INT,
    OUT tenant_attribute TEXT,
    OUT read_count_in_this_period INT,
    OUT read_count_in_last_period INT,
    OUT query_count_in_this_period INT,
    OUT query_count_in_last_period INT,
    OUT score BIGINT
)
    RETURNS SETOF record
    LANGUAGE plpgsql
    AS $function$
BEGIN
    IF
        array_position(enumvals, 'log') >= array_position(enumvals, setting)
        AND setting != 'off'
        FROM pg_settings
        WHERE name = 'citus.multi_tenant_monitoring_log_level'
    THEN
        RAISE LOG 'Generating citus_stats_tenants';
    END IF;
    RETURN QUERY
    SELECT *
    FROM jsonb_to_recordset((
        SELECT
            jsonb_agg(all_cst_rows_as_jsonb.cst_row_as_jsonb)::jsonb
        FROM (
            SELECT
                jsonb_array_elements(run_command_on_all_nodes.result::jsonb)::jsonb ||
                    ('{"nodeid":' || run_command_on_all_nodes.nodeid || '}')::jsonb AS cst_row_as_jsonb
            FROM
                run_command_on_all_nodes (
                    $$
                        SELECT
                            coalesce(to_jsonb (array_agg(cstl.*)), '[]'::jsonb)
                        FROM citus_stats_tenants_local($$||return_all_tenants||$$) cstl;
                    $$,
                    parallel:= TRUE,
                    give_warning_for_connection_errors:= TRUE)
            WHERE
                success = 't')
        AS all_cst_rows_as_jsonb))
AS (
    nodeid INT,
    colocation_id INT,
    tenant_attribute TEXT,
    read_count_in_this_period INT,
    read_count_in_last_period INT,
    query_count_in_this_period INT,
    query_count_in_last_period INT,
    score BIGINT
)
    ORDER BY score DESC
    LIMIT CASE WHEN NOT return_all_tenants THEN current_setting('citus.stats_tenants_limit')::BIGINT END;
END;
$function$;

CREATE OR REPLACE VIEW citus.citus_stats_tenants AS
SELECT
    nodeid,
    colocation_id,
    tenant_attribute,
    read_count_in_this_period,
    read_count_in_last_period,
    query_count_in_this_period,
    query_count_in_last_period
FROM pg_catalog.citus_stats_tenants(FALSE);

ALTER VIEW citus.citus_stats_tenants SET SCHEMA pg_catalog;

REVOKE ALL ON FUNCTION pg_catalog.citus_stats_tenants(BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.citus_stats_tenants(BOOLEAN) TO pg_monitor;

REVOKE ALL ON pg_catalog.citus_stats_tenants FROM PUBLIC;
GRANT SELECT ON pg_catalog.citus_stats_tenants TO pg_monitor;
