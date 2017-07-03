/* citus--7.0-3--7.0-4.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION assign_distributed_transaction_id(initiator_node_identifier int4, transaction_number int8, transaction_stamp timestamptz)
     RETURNS void
     LANGUAGE C STRICT
     AS 'MODULE_PATHNAME',$$assign_distributed_transaction_id$$;
 COMMENT ON FUNCTION assign_distributed_transaction_id(initiator_node_identifier int4, transaction_number int8, transaction_stamp timestamptz)
     IS 'Only intended for internal use, users should not call this. The function sets the distributed transaction id';

CREATE OR REPLACE FUNCTION get_current_transaction_id(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     RETURNS RECORD
     LANGUAGE C STRICT
     AS 'MODULE_PATHNAME',$$get_current_transaction_id$$;
 COMMENT ON FUNCTION get_current_transaction_id(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns the current backend data including distributed transaction id';

CREATE OR REPLACE FUNCTION get_all_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
	RETURNS SETOF RECORD
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$get_all_active_transactions$$;
 COMMENT ON FUNCTION get_all_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns distributed transaction ids of active distributed transactions';

RESET search_path;
