--
-- MULTI_MX_REPARTITION_W1_UDT
--

\c - - :public_worker_1_host :worker_1_port
SET client_min_messages = LOG;
-- Query that should result in a repartition join on UDT column.
SET citus.task_executor_type = 'task-tracker';
SET citus.log_multi_join_order = true;

-- Query that should result in a repartition
-- join on int column, and be empty
SELECT * FROM repartition_udt JOIN repartition_udt_other
    ON repartition_udt.pk = repartition_udt_other.pk;

SELECT * FROM repartition_udt JOIN repartition_udt_other
    ON repartition_udt.udtcol = repartition_udt_other.udtcol
	WHERE repartition_udt.pk > 1
	ORDER BY repartition_udt.pk;
