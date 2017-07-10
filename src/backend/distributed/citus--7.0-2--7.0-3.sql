/* citus--7.0-2--7.0-3.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION assign_distributed_transaction_id(originNodeId int8, transactionId int8, transactionStamp timestamptz)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$assign_distributed_transaction_id$$;
COMMENT ON FUNCTION assign_distributed_transaction_id(originNodeId int8, transactionId int8, transactionStamp timestamptz)
    IS 'sets distributed transaction id';

CREATE FUNCTION pg_catalog.dump_local_wait_edges(
    IN source_node_id int, IN local_node_id int,
    OUT waiting_pid int4,
    OUT waiting_node_id int4, OUT waiting_transaction_id int8, OUT waiting_transaction_stamp timestamptz,
    OUT blocking_pid int,
    OUT blocking_nodeid int4, OUT blocking_transactionid int8, OUT blocking_transactionstamp timestamptz,
    OUT blocking_also_blocked bool)
RETURNS SETOF RECORD
LANGUAGE 'c'
STRICT
AS $$citus$$, $$dump_local_wait_edges$$;

CREATE FUNCTION pg_catalog.dump_all_wait_edges(
    OUT pid int4,
    OUT nodeId int8, OUT transactionId int8, OUT transactionStamp timestamptz,
    OUT blocked_pid int,
    OUT blocked_nodeid int8, OUT blocked_transactionid int8, OUT blocked_transactionstamp timestamptz,
    OUT blocked_blocked bool)
RETURNS SETOF RECORD
LANGUAGE 'c'
STRICT
AS $$citus$$, $$dump_all_wait_edges$$;


CREATE FUNCTION pg_catalog.this_machine_kills_deadlocks()
RETURNS bool
LANGUAGE 'c'
STRICT
AS $$citus$$, $$this_machine_kills_deadlocks$$;

RESET search_path;
