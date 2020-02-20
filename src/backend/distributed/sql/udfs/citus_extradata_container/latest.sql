-- we use the citus_extradata_container function as a range table entry in the query part
-- executed on the coordinator. Now that we are letting this query be planned by the
-- postgres planner we need to be able to pass column names and type information with this
-- function. This requires the change of the prototype of the function and add a return
-- type. Changing the return type of the function requires we drop the function first.
DROP FUNCTION citus_extradata_container(INTERNAL);
CREATE OR REPLACE FUNCTION citus_extradata_container(INTERNAL)
    RETURNS SETOF record
    LANGUAGE C
AS 'MODULE_PATHNAME', $$citus_extradata_container$$;
COMMENT ON FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    IS 'placeholder function to store additional data in postgres node trees';
