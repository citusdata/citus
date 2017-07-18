# Tests around distributed transaction id generation

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

step "s1-get-current-transaction-id"
{
	SELECT row(initiator_node_identifier, transaction_number) FROM  get_current_transaction_id();
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

step "s2-commit"
{
    COMMIT;
}

# print only the necessary parts to prevent concurrent runs to print different values
step "s2-get-first-worker-active-transactions"
{
		SELECT * FROM run_command_on_workers('SELECT row(initiator_node_identifier, transaction_number)
												FROM	 
											  get_all_active_transactions();
											') 
		WHERE nodeport = 57637;
;
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

session "s4"

step "s4-get-all-transactions"
{
	SELECT initiator_node_identifier, transaction_number, transaction_stamp FROM get_all_active_transactions() ORDER BY 1,2,3;
}

# show that we could get all distributed transaction ids from seperate sessions
permutation "s1-begin" "s1-assign-transaction-id" "s4-get-all-transactions" "s2-begin" "s2-assign-transaction-id" "s4-get-all-transactions" "s3-begin" "s3-assign-transaction-id" "s4-get-all-transactions" "s1-commit" "s4-get-all-transactions" "s2-commit" "s4-get-all-transactions" "s3-commit" "s4-get-all-transactions"


# now show that distributed transaction id on the coordinator
# is the same with the one on the worker
permutation "s1-create-table" "s1-begin" "s1-insert" "s1-get-current-transaction-id" "s2-get-first-worker-active-transactions"

