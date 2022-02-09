DROP FUNCTION pg_catalog.dump_local_wait_edges CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.dump_local_wait_edges(
					distributed_tx_only boolean DEFAULT true,
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
AS $$MODULE_PATHNAME$$, $$dump_local_wait_edges$$;
COMMENT ON FUNCTION pg_catalog.dump_local_wait_edges(bool)
IS 'returns all local lock wait chains, that start from any citus backend';
