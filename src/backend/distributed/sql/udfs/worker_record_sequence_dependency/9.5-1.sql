CREATE FUNCTION pg_catalog.worker_record_sequence_dependency(seq_name regclass, table_name regclass, column_name name)
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', 'worker_record_sequence_dependency';
COMMENT ON FUNCTION pg_catalog.worker_record_sequence_dependency(regclass,regclass,name)
  IS 'record the fact that the sequence depends on the table in pg_depend';

REVOKE ALL ON FUNCTION pg_catalog.worker_record_sequence_dependency(regclass,regclass,name)
  FROM PUBLIC;
