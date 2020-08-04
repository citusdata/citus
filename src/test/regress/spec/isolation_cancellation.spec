// Tests around cancelling statements. As we can't trigger cancel
// interrupts directly, we use statement_timeout instead, which largely
// behaves the same as proper cancellation.

setup
{
    CREATE TABLE cancel_table (test_id integer NOT NULL, data text);
    SELECT create_distributed_table('cancel_table', 'test_id', 'hash');
    INSERT INTO cancel_table VALUES(1);
}

teardown
{
    DROP TABLE IF EXISTS cancel_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-rollback"
{
    ROLLBACK;
}

step "s1-sleep10000"
{
    SELECT pg_sleep(10000) FROM cancel_table WHERE test_id = 1;
}

step "s1-timeout"
{
    SET statement_timeout = '100ms';
}

step "s1-update1"
{
    UPDATE cancel_table SET data = '' WHERE test_id = 1;
}

step "s1-reset"
{
    RESET ALL;
}

step "s1-drop"
{

    DROP TABLE cancel_table;
}

session "s2"

step "s2-drop"
{

    DROP TABLE cancel_table;
}

// check that statement cancel works for plain selects, drop table
// afterwards to make sure sleep on workers is cancelled (thereby not
// preventing drop via locks)
permutation "s1-timeout" "s1-sleep10000" "s1-reset" "s1-drop"
permutation "s1-timeout" "s1-sleep10000" "s1-reset" "s2-drop"

// check that statement cancel works for selects in transaction
permutation "s1-timeout" "s1-begin" "s1-sleep10000" "s1-rollback" "s1-reset" "s1-drop"
permutation "s1-timeout" "s1-begin" "s1-sleep10000" "s1-rollback" "s1-reset" "s2-drop"

// check that statement cancel works for selects in transaction, that previously wrote
permutation "s1-timeout" "s1-begin" "s1-update1" "s1-sleep10000" "s1-rollback" "s1-reset" "s1-drop"
permutation "s1-timeout" "s1-begin" "s1-update1" "s1-sleep10000" "s1-rollback" "s1-reset" "s2-drop"
