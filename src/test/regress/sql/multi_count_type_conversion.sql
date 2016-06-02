--
-- MULTI_COUNT_TYPE_CONVERSION
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 400000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 400000;


-- Verify that we can sort count(*) results correctly. We perform this check as
-- our count() operations execute in two steps: worker nodes report their
-- count() results, and the master node sums these counts up. During this sum(),
-- the data type changes from int8 to numeric. When we sort the numeric value,
-- we get erroneous results on 64-bit architectures. To fix this issue, we
-- manually cast back the sum() result to an int8 data type.

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity ASC, l_quantity DESC;

SELECT count(*) count_quantity, l_quantity FROM lineitem WHERE l_quantity < 32.0
	GROUP BY l_quantity
	ORDER BY count_quantity DESC, l_quantity ASC;
