setup
{
    CREATE FUNCTION run_pg_send_cancellation(int,int)
    RETURNS void
    AS 'citus'
    LANGUAGE C STRICT;

    CREATE FUNCTION get_cancellation_key()
    RETURNS int
    AS 'citus'
    LANGUAGE C STRICT;

    CREATE TABLE cancel_table (pid int, cancel_key int);
}

teardown
{
    DROP TABLE IF EXISTS cancel_table;
}

session "s1"

/* store the PID and cancellation key of session 1 */
step "s1-register"
{
    INSERT INTO cancel_table VALUES (pg_backend_pid(), get_cancellation_key());
}

/* lock the table from session 1, will block and get cancelled */
step "s1-lock"
{
    BEGIN;
    LOCK TABLE cancel_table IN ACCESS EXCLUSIVE MODE;
    END;
}

session "s2"

/* lock the table from session 2 to block session 1 */
step "s2-lock"
{
    BEGIN;
    LOCK TABLE cancel_table IN ACCESS EXCLUSIVE MODE;
}

/* PID mismatch */
step "s2-wrong-cancel-1"
{
    SELECT run_pg_send_cancellation(pid + 1, cancel_key) FROM cancel_table;
}

/* cancellation key mismatch */
step "s2-wrong-cancel-2"
{
    SELECT run_pg_send_cancellation(pid, cancel_key + 1) FROM cancel_table;
}

/* cancel the LOCK statement in session 1 */
step "s2-cancel"
{
    SELECT run_pg_send_cancellation(pid, cancel_key) FROM cancel_table;
    END;
}

permutation "s1-register" "s2-lock" "s1-lock" "s2-wrong-cancel-1" "s2-wrong-cancel-2" "s2-cancel"
