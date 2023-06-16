DO $$
declare
citus_schemas_create_query text;
BEGIN
citus_schemas_create_query=$CSCQ$
    CREATE OR REPLACE VIEW %I.citus_schemas AS
    SELECT
        ts.schemaid::regnamespace AS schema_name,
        ts.colocationid AS colocation_id,
        CASE
            WHEN pg_catalog.has_schema_privilege(CURRENT_USER, ts.schemaid::regnamespace, 'USAGE')
            THEN pg_size_pretty(coalesce(schema_sizes.schema_size, 0))
            ELSE NULL
        END AS schema_size,
        pg_get_userbyid(n.nspowner) AS schema_owner
    FROM
        pg_dist_schema ts
    JOIN
        pg_namespace n ON (ts.schemaid = n.oid)
    LEFT JOIN (
        SELECT c.relnamespace::regnamespace schema_id, SUM(size) AS schema_size
        FROM citus_shard_sizes() css, pg_dist_shard ds, pg_class c
        WHERE css.shard_id = ds.shardid AND ds.logicalrelid = c.oid
        GROUP BY schema_id
    ) schema_sizes ON schema_sizes.schema_id = ts.schemaid
    ORDER BY
        schema_name;
$CSCQ$;

IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public') THEN
    EXECUTE format(citus_schemas_create_query, 'public');
    REVOKE ALL ON public.citus_schemas FROM public;
    GRANT SELECT ON public.citus_schemas TO public;
ELSE
    EXECUTE format(citus_schemas_create_query, 'citus');
    ALTER VIEW citus.citus_schemas SET SCHEMA pg_catalog;
    REVOKE ALL ON pg_catalog.citus_schemas FROM public;
    GRANT SELECT ON pg_catalog.citus_schemas TO public;
END IF;

END;
$$;
