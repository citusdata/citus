setup
{
    CREATE TABLE distributed_table (x int primary key, y int);
    SELECT create_distributed_table('distributed_table', 'x');
    INSERT INTO distributed_table VALUES (1,0);

    CREATE OR REPLACE FUNCTION get_adjacency_list_wait_graph(OUT transactionNumber int, OUT waitingTransactionNumbers cstring)
    RETURNS SETOF RECORD
    LANGUAGE C STRICT
    AS 'citus', $$get_adjacency_list_wait_graph$$;
    COMMENT ON FUNCTION get_adjacency_list_wait_graph(OUT transactionNumber int, OUT waitingTransactionNumbers cstring)
    IS 'returns flattened wait graph';
}

teardown
{
    DROP TABLE distributed_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-update"
{
    UPDATE distributed_table SET y = 1 WHERE x = 1;
}

step "s1-abort"
{
    ABORT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-update"
{
    UPDATE distributed_table SET y = 2 WHERE x = 1;
}

step "s2-abort"
{
    ABORT;
}

session "s3"

step "s3-begin"
{
    BEGIN;
}

step "s3-update"
{
    UPDATE distributed_table SET y = 3 WHERE x = 1;
}

step "s3-abort"
{
    ABORT;
}


session "detector"

step "detector-dump-wait-edges"
{
    SELECT
        waiting_transaction_num,
        blocking_transaction_num,
        blocking_transaction_waiting
    FROM
        dump_global_wait_edges()
    ORDER BY
        waiting_transaction_num,
        blocking_transaction_num,
        blocking_transaction_waiting;

    SELECT * FROM get_adjacency_list_wait_graph() ORDER BY 1;
}

// Distributed transaction blocked by another distributed transaction
permutation "s1-begin" "s2-begin" "s1-update" "s2-update" "detector-dump-wait-edges" "s1-abort" "s2-abort"

// Distributed transaction blocked by another distributed transaction blocked by another distributed transaction
permutation "s1-begin" "s2-begin" "s3-begin" "s1-update" "s2-update" "s3-update" "detector-dump-wait-edges" "s1-abort" "s2-abort" "s3-abort"
