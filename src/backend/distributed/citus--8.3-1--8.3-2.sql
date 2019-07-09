/* citus--8.3-1--8.3-2 */

CREATE SCHEMA citus_internal;

/*
 * We redundantly keep track of both OID and names in order to survive
 * pg_upgrade, since function OIDs might change.
 *
 * TODO: handle function renames
 */
CREATE TABLE citus_internal.procedures (
    procedure_id oid not null,
    schema_name regnamespace not null,
    procedure_name name not null,
    distribution_arg_index int not null,
    colocation_id int not null
);

-- create a primary key index with a known name
CREATE UNIQUE INDEX procedures_procedure_id_idx ON citus_internal.procedures (procedure_id);
ALTER TABLE citus_internal.procedures
ADD CONSTRAINT procedures_pkey PRIMARY KEY USING INDEX procedures_procedure_id_idx;

SELECT pg_catalog.pg_extension_config_dump('citus_internal.procedures', '');

-- update OIDs after pg_upgrade (TODO: test)
CREATE OR REPLACE FUNCTION citus_internal.procedure_insert_oid()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    SELECT oid INTO NEW.procedure_id
    FROM pg_proc
    WHERE proname = NEW.procedure_name
    AND pronamespace = NEW.schema_name;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER procedure_insert_oid
BEFORE INSERT ON citus_internal.procedures
FOR EACH ROW EXECUTE PROCEDURE citus_internal.procedure_insert_oid();

CREATE FUNCTION pg_catalog.distribute_procedure(procedure_name regprocedure,
                                                distribution_arg text DEFAULT NULL,
                                                colocate_with text DEFAULT 'default')
RETURNS bool
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$distribute_procedure$$;
COMMENT ON FUNCTION pg_catalog.distribute_procedure(regprocedure, text, colocate_with text)
IS 'replicate a procedure on all nodes';
