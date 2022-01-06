DROP FUNCTION IF EXISTS get_global_active_transactions();
CREATE FUNCTION get_global_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT global_pid int8)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$get_global_active_transactions$$;
COMMENT ON FUNCTION get_global_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT global_pid int8)
     IS 'returns distributed transaction ids of active distributed transactions from each node of the cluster';
