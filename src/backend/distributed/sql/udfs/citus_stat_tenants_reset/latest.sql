CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_tenants_reset()
    RETURNS VOID
    LANGUAGE plpgsql
    SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM run_command_on_all_nodes($$SELECT citus_stat_tenants_local_reset()$$);
END;
$function$;
