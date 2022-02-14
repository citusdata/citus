CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_global_blocked_processes(
				OUT waiting_global_pid int8,
                    OUT waiting_pid int4,
                    OUT waiting_node_id int4,
                    OUT waiting_transaction_num int8,
                    OUT waiting_transaction_stamp timestamptz,
				OUT blocking_global_pid int8,
                    OUT blocking_pid int4,
                    OUT blocking_node_id int4,
                    OUT blocking_transaction_num int8,
                    OUT blocking_transaction_stamp timestamptz,
                    OUT blocking_transaction_waiting bool)
RETURNS SETOF RECORD
LANGUAGE C STRICT
AS $$MODULE_PATHNAME$$, $$citus_internal_global_blocked_processes$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_global_blocked_processes()
IS 'returns a global list of blocked backends originating from this node';
