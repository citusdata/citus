CREATE OR REPLACE FUNCTION create_distributed_function(function_name regprocedure)
  RETURNS void
  LANGUAGE C CALLED ON NULL INPUT
  AS 'MODULE_PATHNAME', $$create_distributed_function$$;
COMMENT ON FUNCTION create_distributed_function(function_name regprocedure)
  IS 'creates a distributed function';
  