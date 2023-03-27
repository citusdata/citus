CREATE OR REPLACE FUNCTION pg_catalog.citus_stats_tenants(
    return_all_tenants BOOLEAN DEFAULT FALSE,
    OUT colocation_id INT,
    OUT tenant_attribute TEXT,
    OUT read_count_in_this_period INT,
    OUT read_count_in_last_period INT,
    OUT query_count_in_this_period INT,
    OUT query_count_in_last_period INT,
    OUT score BIGINT)
RETURNS SETOF RECORD
LANGUAGE C
AS 'citus', $$citus_stats_tenants$$;


CREATE OR REPLACE VIEW citus.citus_stats_tenants AS
SELECT
    colocation_id,
    tenant_attribute,
    read_count_in_this_period,
    read_count_in_last_period,
    query_count_in_this_period,
    query_count_in_last_period
FROM pg_catalog.citus_stats_tenants()
ORDER BY score DESC;

ALTER VIEW citus.citus_stats_tenants SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stats_tenants TO PUBLIC;
