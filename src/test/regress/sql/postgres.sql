-- We override the distribute functions so that we can test postgres configs easily.

CREATE OR REPLACE FUNCTION pg_catalog.create_distributed_table(table_name regclass,
                                                    distribution_column text,
                                                    distribution_type citus.distribution_type DEFAULT 'hash',
                                                    colocate_with text DEFAULT 'default',
                                                    shard_count int DEFAULT NULL)
    RETURNS void
	LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

CREATE OR REPLACE FUNCTION pg_catalog.create_reference_table(table_name regclass)
	RETURNS void
	LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

CREATE OR REPLACE FUNCTION pg_catalog.citus_add_local_table_to_metadata(table_name regclass, cascade_via_foreign_keys boolean default false)
	RETURNS void
	LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

CREATE OR REPLACE FUNCTION pg_catalog.create_distributed_function (
    function_name regprocedure,
    distribution_arg_name text DEFAULT NULL,
    colocate_with text DEFAULT 'default',
    force_delegation bool DEFAULT NULL
)
    RETURNS void
    LANGUAGE plpgsql
    CALLED ON NULL INPUT
    AS $function$
        BEGIN
        END;
    $function$;
