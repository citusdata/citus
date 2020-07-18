--
-- MULTI_BINARY_MASTER_COPY
--


SET citus.next_shard_id TO 430000;


-- Try binary master copy for different executors

SET citus.binary_master_copy_format TO 'on';

SELECT count(*) FROM lineitem;
SELECT l_shipmode FROM lineitem WHERE l_partkey = 67310 OR l_partkey = 155190;


SELECT count(*) FROM lineitem;
SELECT l_shipmode FROM lineitem WHERE l_partkey = 67310 OR l_partkey = 155190;
