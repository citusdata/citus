CREATE OR REPLACE FUNCTION truncate_local_data_after_distributing_table(function_name regclass)
  RETURNS void
  LANGUAGE C CALLED ON NULL INPUT
  AS 'MODULE_PATHNAME', $$truncate_local_data_after_distributing_table$$;

COMMENT ON FUNCTION truncate_local_data_after_distributing_table(function_name regclass)
  IS 'truncates local records of a distributed table';
