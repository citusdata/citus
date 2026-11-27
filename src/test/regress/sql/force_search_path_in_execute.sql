-- Verify that privileged internal queries do not resolve operators from the
-- session search_path.
BEGIN;

SET citus.create_object_propagation TO deferred;

CREATE SCHEMA force_search_path_in_execute;
CREATE TABLE force_search_path_in_execute.operator_calls(value boolean);

CREATE FUNCTION force_search_path_in_execute.oid_eq(oid, oid)
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO force_search_path_in_execute.operator_calls VALUES (true);
    RETURN pg_catalog.oideq($1, $2);
END;
$$;

CREATE OPERATOR force_search_path_in_execute.= (
    LEFTARG = oid,
    RIGHTARG = oid,
    FUNCTION = force_search_path_in_execute.oid_eq
);

CREATE FUNCTION force_search_path_in_execute.drop_target()
RETURNS integer
LANGUAGE sql
AS $$ SELECT 1 $$;

SELECT create_distributed_function(
    'force_search_path_in_execute.drop_target()'::regprocedure
);

SET search_path TO force_search_path_in_execute, pg_catalog;
DROP FUNCTION force_search_path_in_execute.drop_target();
SET search_path TO pg_catalog;

SELECT count(*) AS untrusted_operator_calls
FROM force_search_path_in_execute.operator_calls;

ROLLBACK;
