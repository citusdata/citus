// Tests around distributed transaction id generation

setup
{
	SET TIME ZONE 'PST8PDT';
}

teardown
{
	SET TIME ZONE DEFAULT;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(1, 1, '2015-01-01 00:00:00+0');
}

step "s1-has-transaction-number"
{
    SELECT transaction_number > 0 FROM get_current_transaction_id();
}

step "s1-commit"
{
    COMMIT;
}

step "s1-create-table"
{
	-- some tests also use distributed table
	CREATE TABLE distributed_transaction_id_table(some_value int, other_value int);
	SET citus.shard_count TO 4;
	SELECT create_distributed_table('distributed_transaction_id_table', 'some_value');
}

step "s1-insert"
{
	INSERT INTO distributed_transaction_id_table VALUES (1, 1);
}

step "s1-verify-current-xact-is-on-worker"
{
	SELECT
	    remote.nodeport,
	    remote.result = row(xact.transaction_number)::text AS xact_exists
	FROM
	    get_current_transaction_id() as xact,
	    run_command_on_workers($$
	        SELECT row(transaction_number)
            FROM get_all_active_transactions()
			WHERE transaction_number != 0;
        $$) as remote
    ORDER BY remote.nodeport ASC;
}

step "s1-get-all-transactions"
{
	SELECT initiator_node_identifier, transaction_number, transaction_stamp FROM get_current_transaction_id() ORDER BY 1,2,3;
}

step "s1-drop-table"
{
	DROP TABLE distributed_transaction_id_table;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(2, 2, '2015-01-02 00:00:00+0');
}

step "s2-vacuum"
{
    VACUUM FULL pg_dist_partition;
}

step "s2-commit"
{
    COMMIT;
}

step "s2-get-all-transactions"
{
	SELECT initiator_node_identifier, transaction_number, transaction_stamp FROM get_current_transaction_id() ORDER BY 1,2,3;
}

session "s3"

step "s3-begin"
{
    BEGIN;
}

step "s3-assign-transaction-id"
{
    SELECT assign_distributed_transaction_id(3, 3, '2015-01-03 00:00:00+0');
}

step "s3-commit"
{
    COMMIT;
}

step "s3-get-all-transactions"
{
	SELECT initiator_node_identifier, transaction_number, transaction_stamp FROM get_current_transaction_id() ORDER BY 1,2,3;
}

// show that we could get all distributed transaction ids from seperate sessions
permutation "s1-begin" "s1-assign-transaction-id" "s1-get-all-transactions" "s2-begin" "s2-assign-transaction-id" "s2-get-all-transactions" "s3-begin" "s3-assign-transaction-id" "s3-get-all-transactions" "s1-commit" "s2-commit" "s3-commit"


// now show that distributed transaction id on the coordinator
// is the same with the one on the worker
permutation "s1-create-table" "s1-begin" "s1-insert" "s1-verify-current-xact-is-on-worker" "s1-drop-table" "s1-commit"

// we would initially forget the distributed transaction ID on pg_dist_partition invalidations
permutation "s1-begin" "s1-assign-transaction-id" "s1-has-transaction-number" "s2-vacuum" "s1-has-transaction-number" "s1-commit"
