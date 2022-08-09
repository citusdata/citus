--
-- MULTI_STAGE_MORE_DATA
--


SET citus.next_shard_id TO 280000;


-- We load more data to customer and part tables to test distributed joins. The
-- loading causes the planner to consider customer and part tables as large, and
-- evaluate plans where some of the underlying tables need to be repartitioned.

\set customer_2_data_file :abs_srcdir '/data/customer.2.data'
\set customer_3_data_file :abs_srcdir '/data/customer.3.data'
\set part_more_data_file :abs_srcdir '/data/part.more.data'
\set client_side_copy_command '\\copy customer FROM ' :'customer_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy customer FROM ' :'customer_3_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy part FROM ' :'part_more_data_file' ' with delimiter '''|''';'
:client_side_copy_command

SELECT master_create_empty_shard('customer_append') AS shardid1 \gset
SELECT master_create_empty_shard('customer_append') AS shardid2 \gset

copy customer_append FROM :'customer_2_data_file' with (delimiter '|', append_to_shard :shardid1);
copy customer_append FROM :'customer_3_data_file' with (delimiter '|', append_to_shard :shardid2);

SELECT master_create_empty_shard('part_append') AS shardid \gset

copy part_append FROM :'part_more_data_file' with (delimiter '|', append_to_shard :shardid);
