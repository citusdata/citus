-- Test behaviour of repartitioned INSERT ... SELECT in MX setup

CREATE SCHEMA multi_mx_insert_select_repartition;
SET search_path TO multi_mx_insert_select_repartition;

SET citus.next_shard_id TO 4213581;
SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;

SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT floor(i/4), i*i FROM generate_series(1, 20) i;

SET citus.shard_count TO 3;
CREATE TABLE target_table(a int, b int);
SELECT create_distributed_table('target_table', 'a');

CREATE FUNCTION square(int) RETURNS INT
    AS $$ SELECT $1 * $1 $$
    LANGUAGE SQL;

select create_distributed_function('square(int)');
select public.colocate_proc_with_table('square', 'source_table'::regclass, 0);

-- Test along with function delegation
-- function delegation only happens for "SELECT f()", and we don't use
-- repartitioned INSERT/SELECT when task count is 1, so the following
-- should go via coordinator
EXPLAIN (costs off) INSERT INTO target_table(a) SELECT square(4);
INSERT INTO target_table(a) SELECT square(4);
SELECT * FROM target_table;

TRUNCATE target_table;

--
-- Test repartitioned INSERT/SELECT from MX worker
--
\c - - :public_worker_1_host :worker_1_port
SET search_path TO multi_mx_insert_select_repartition;
EXPLAIN (costs off) INSERT INTO target_table SELECT a, max(b) FROM source_table GROUP BY a;
INSERT INTO target_table SELECT a, max(b) FROM source_table GROUP BY a;

\c - - :master_host :master_port
SET search_path TO multi_mx_insert_select_repartition;
SELECT * FROM target_table ORDER BY a;

RESET client_min_messages;
\set VERBOSITY terse
DROP SCHEMA multi_mx_insert_select_repartition CASCADE;
