SET search_path = 'pg_catalog';
DROP VIEW IF EXISTS pg_catalog.citus_nodes;

CREATE OR REPLACE VIEW citus.citus_nodes AS
SELECT
    nodename,
    nodeport,
    CASE
        WHEN groupid = 0 THEN 'coordinator'
        ELSE 'worker'
    END AS role,
    isactive AS active
FROM pg_dist_node;

ALTER VIEW citus.citus_nodes SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_nodes TO PUBLIC;

RESET search_path;
