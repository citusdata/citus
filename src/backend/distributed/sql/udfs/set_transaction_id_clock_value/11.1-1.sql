CREATE OR REPLACE FUNCTION citus_internal.set_transaction_id_clock_value(bigint)
  RETURNS VOID
  LANGUAGE C STABLE PARALLEL SAFE STRICT
  AS 'MODULE_PATHNAME', $$set_transaction_id_clock_value$$;
COMMENT ON FUNCTION citus_internal.set_transaction_id_clock_value(bigint)
     IS 'Internal UDF to set the transaction clock value of the remote node(s) distributed transaction id';
