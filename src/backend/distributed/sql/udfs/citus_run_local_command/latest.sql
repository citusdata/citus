CREATE OR REPLACE FUNCTION pg_catalog.citus_run_local_command(command text)
RETURNS void AS $$
BEGIN
    EXECUTE $1;
END;
$$ LANGUAGE PLPGSQL
SET search_path = pg_catalog, pg_temp;
COMMENT ON FUNCTION pg_catalog.citus_run_local_command(text)
    IS 'citus_run_local_command executes the input command';
