DROP FUNCTION pg_catalog.create_distributed_function(regprocedure, text, text);

CREATE OR REPLACE FUNCTION pg_catalog.create_distributed_function(function_name regprocedure,
						       distribution_arg_name text DEFAULT NULL,
						       colocate_with text DEFAULT 'default',
						       force_delegation bool DEFAULT NULL)
  RETURNS void
  LANGUAGE C CALLED ON NULL INPUT
  AS 'MODULE_PATHNAME', $$create_distributed_function$$;

COMMENT ON FUNCTION pg_catalog.create_distributed_function(function_name regprocedure,
						distribution_arg_name text,
						colocate_with text,
						force_delegation bool)
  IS 'creates a distributed function';
