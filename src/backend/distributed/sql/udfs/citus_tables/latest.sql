DO $$
declare
citus_tables_create_query text;
BEGIN
citus_tables_create_query=$CTCQ$
    CREATE OR REPLACE VIEW %I.citus_tables AS
    SELECT
        logicalrelid AS table_name,
        CASE WHEN partkey IS NOT NULL THEN 'distributed' ELSE
            CASE when repmodel = 't' THEN 'reference' ELSE 'local' END
        END AS citus_table_type,
        coalesce(column_to_column_name(logicalrelid, partkey), '<none>') AS distribution_column,
        colocationid AS colocation_id,
        pg_size_pretty(citus_total_relation_size(logicalrelid, fail_on_error := false)) AS table_size,
        (select count(*) from pg_dist_shard where logicalrelid = p.logicalrelid) AS shard_count,
        pg_get_userbyid(relowner) AS table_owner,
        amname AS access_method
    FROM
        pg_dist_partition p
    JOIN
        pg_class c ON (p.logicalrelid = c.oid)
    LEFT JOIN
        pg_am a ON (a.oid = c.relam)
    WHERE
        -- filter out tables owned by extensions
        logicalrelid NOT IN (
            SELECT
                objid
            FROM
                pg_depend
            WHERE
                classid = 'pg_class'::regclass AND refclassid = 'pg_extension'::regclass AND deptype = 'e'
        )
    ORDER BY
        logicalrelid::text;
$CTCQ$;

IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'public') THEN
    EXECUTE format(citus_tables_create_query, 'public');
    GRANT SELECT ON public.citus_tables TO public;
ELSE
    EXECUTE format(citus_tables_create_query, 'citus');
    ALTER VIEW citus.citus_tables SET SCHEMA pg_catalog;
    GRANT SELECT ON pg_catalog.citus_tables TO public;
END IF;

END;
$$;
