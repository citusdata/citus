--
-- MULTI_DISTRIBUTED_TRANSACTION_ID
-- 
-- Unit tests for distributed transaction id functionality
--

-- get the current transaction id, which should be uninitialized
-- note that we skip printing the databaseId, which might change
-- per run

-- set timezone to a specific value to prevent
-- different values on different servers
SET TIME ZONE 'PST8PDT';

-- should return uninitialized values if not in a transaction
SELECT initiator_node_identifier, transaction_number, transaction_stamp FROM get_current_transaction_id();

BEGIN;
	
	-- we should still see the uninitialized values
	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

	-- now assign a value
    SELECT assign_distributed_transaction_id(50, 50, '2016-01-01 00:00:00+0');

    -- see the assigned value
	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

	-- a backend cannot be assigned another tx id if already assigned
    SELECT assign_distributed_transaction_id(51, 51, '2017-01-01 00:00:00+0');

ROLLBACK;

-- since the transaction finished, we should see the uninitialized values
SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();


-- also see that ROLLBACK (i.e., failures in the transaction) clears the shared memory
BEGIN;
	
	-- we should still see the uninitialized values
	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

	-- now assign a value
    SELECT assign_distributed_transaction_id(52, 52, '2015-01-01 00:00:00+0');

    SELECT 5 / 0;
COMMIT;

-- since the transaction errored, we should see the uninitialized values again
	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();


-- we should also see that a new connection means an uninitialized transaction id
BEGIN;
	
	SELECT assign_distributed_transaction_id(52, 52, '2015-01-01 00:00:00+0');

	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

	\c - - - :master_port

	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

-- now show that PREPARE resets the distributed transaction id

BEGIN;
	SELECT assign_distributed_transaction_id(120, 120, '2015-01-01 00:00:00+0');

	SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

	PREPARE TRANSACTION 'dist_xact_id_test';

-- after the prepare we should see that transaction id is cleared
SELECT initiator_node_identifier, transaction_number, transaction_stamp, (process_id = pg_backend_pid()) FROM get_current_transaction_id();

-- cleanup the transaction
ROLLBACK PREPARED 'dist_xact_id_test';

-- set back to the original zone
SET TIME ZONE DEFAULT;
