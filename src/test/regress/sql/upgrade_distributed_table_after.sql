SET search_path TO upgrade_distributed_table_before, public;

SELECT * FROM t ORDER BY a;
SELECT * FROM t WHERE a = 1;

DROP SCHEMA upgrade_distributed_table_before CASCADE;
