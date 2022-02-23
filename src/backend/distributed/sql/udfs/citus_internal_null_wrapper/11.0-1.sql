CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_null_wrapper(input anyelement)
 RETURNS anyelement
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN input;
END;
$function$;
COMMENT ON FUNCTION pg_catalog.citus_internal_null_wrapper(input anyelement)
 IS 'returns the input as the same type';
