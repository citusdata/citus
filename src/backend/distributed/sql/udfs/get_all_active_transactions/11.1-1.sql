DROP FUNCTION IF EXISTS pg_catalog.get_all_active_transactions();
CREATE OR REPLACE FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                                  OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                                  OUT global_pid int8)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$get_all_active_transactions$$;

COMMENT ON FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                           OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                           OUT global_pid int8)
IS 'returns transaction information for all Citus initiated transactions';
