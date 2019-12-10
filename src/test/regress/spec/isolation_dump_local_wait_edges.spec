setup
{
    CREATE TABLE local_table (x int primary key, y int);
    INSERT INTO local_table VALUES (1,0);
}

teardown
{
    DROP TABLE local_table;
}

session "dist11"

step "dist11-begin"
{
    BEGIN;
    SELECT assign_distributed_transaction_id(11, 1, '2017-01-01 00:00:00+0');
}

step "dist11-update"
{
    UPDATE local_table SET y = 1 WHERE x = 1;
}

step "dist11-abort"
{
    ABORT;
}

session "local"

step "local-begin"
{
    BEGIN;
}

step "local-update"
{
    UPDATE local_table SET y = 2 WHERE x = 1;
}

step "local-abort"
{
    ABORT;
}

session "dist13"

step "dist13-begin"
{
    BEGIN;
    SELECT assign_distributed_transaction_id(13, 1, '2017-01-01 00:00:00+0');
}

step "dist13-update"
{
    UPDATE local_table SET y = 3 WHERE x = 1;
}

step "dist13-abort"
{
    ABORT;
}


session "detector"

step "detector-dump-wait-edges"
{
    SELECT
        waiting_node_id,
        waiting_transaction_num,
        blocking_node_id,
        blocking_transaction_num,
        blocking_transaction_waiting
    FROM
        dump_local_wait_edges()
    ORDER BY
        waiting_node_id,
        blocking_transaction_num,
        blocking_transaction_waiting;
}

// Distributed transaction blocked by another distributed transaction
permutation "dist11-begin" "dist13-begin" "dist11-update" "dist13-update" "detector-dump-wait-edges" "dist11-abort" "dist13-abort"

// Distributed transaction blocked by a regular transaction
permutation "local-begin" "dist13-begin" "local-update" "dist13-update" "detector-dump-wait-edges" "local-abort" "dist13-abort"

// Distributed transaction blocked by a regular transaction blocked by a distributed transaction
permutation "dist11-begin" "local-begin" "dist13-begin" "dist11-update" "local-update" "dist13-update" "detector-dump-wait-edges" "dist11-abort" "local-abort" "dist13-abort"
