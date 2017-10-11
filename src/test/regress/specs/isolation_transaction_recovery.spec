setup
{
    CREATE TABLE test_transaction_recovery(column1 int, column2 int);
    SELECT create_reference_table('test_transaction_recovery');
}

teardown
{
    DROP TABLE test_transaction_recovery;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-recover"
{
    SELECT recover_prepared_transactions();
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-insert"
{
    INSERT INTO test_transaction_recovery VALUES (1,2);
}

# Recovery and 2PCs should not block each other
permutation "s1-begin" "s1-recover" "s2-insert" "s1-commit"
