CREATE OR REPLACE FUNCTION pg_catalog.any_value_agg ( anyelement, anyelement )
RETURNS anyelement AS $$
        SELECT CASE WHEN $1 IS NULL THEN $2 ELSE $1 END;
$$ LANGUAGE SQL STABLE;

CREATE AGGREGATE pg_catalog.any_value (
        sfunc       = pg_catalog.any_value_agg,
        combinefunc = pg_catalog.any_value_agg,
        basetype    = anyelement,
        stype       = anyelement
);
COMMENT ON AGGREGATE pg_catalog.any_value(anyelement) IS
    'Returns the value of any row in the group. It is mostly useful when you know there will be only 1 element.';

