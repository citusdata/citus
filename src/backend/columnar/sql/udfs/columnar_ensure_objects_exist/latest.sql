-- citus_internal.columnar_ensure_objects_exist is an internal helper function to create
-- missing objects, anything related to the table access methods.
-- Since the API for table access methods is only available in PG12 we can't create these
-- objects when Citus is installed in PG11. Once citus is installed on PG11 the user can
-- upgrade their database to PG12. Now they require the table access method objects that
-- we couldn't create before.
-- This internal function is called from `citus_finish_pg_upgrade` which the user is
-- required to call after a PG major upgrade.
CREATE OR REPLACE FUNCTION citus_internal.columnar_ensure_objects_exist()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
AS $ceoe$
BEGIN

-- when postgres is version 12 or above we need to create the tableam. If the tableam
-- exist we assume all objects have been created.
IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
IF NOT EXISTS (SELECT 1 FROM pg_am WHERE amname = 'columnar') THEN

#include "../columnar_handler/10.0-1.sql"

#include "../alter_columnar_table_set/10.0-1.sql"

#include "../alter_columnar_table_reset/10.0-1.sql"

    -- add the missing objects to the extension
    ALTER EXTENSION citus ADD FUNCTION columnar.columnar_handler(internal);
    ALTER EXTENSION citus ADD ACCESS METHOD columnar;
    ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_set(
        table_name regclass,
        chunk_group_row_limit int,
        stripe_row_limit int,
        compression name,
        compression_level int);
    ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_reset(
        table_name regclass,
        chunk_group_row_limit bool,
        stripe_row_limit bool,
        compression bool,
        compression_level bool);

END IF;
END IF;
END;
$ceoe$;

COMMENT ON FUNCTION citus_internal.columnar_ensure_objects_exist()
    IS 'internal function to be called by pg_catalog.citus_finish_pg_upgrade responsible for creating the columnar objects';
