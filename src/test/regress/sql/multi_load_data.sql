--
-- MULTI_LOAD_DATA
--

\set lineitem_1_data_file :abs_srcdir '/data/lineitem.1.data'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem.2.data'
\set client_side_copy_command '\\copy lineitem FROM ' :'lineitem_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy lineitem FROM ' :'lineitem_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\set orders_1_data_file :abs_srcdir '/data/orders.1.data'
\set orders_2_data_file :abs_srcdir '/data/orders.2.data'
\set client_side_copy_command '\\copy orders FROM ' :'orders_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders FROM ' :'orders_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\set client_side_copy_command '\\copy orders_reference FROM ' :'orders_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders_reference FROM ' :'orders_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

\set customer_1_data_file :abs_srcdir '/data/customer.1.data'
\set nation_data_file :abs_srcdir '/data/nation.data'
\set part_data_file :abs_srcdir '/data/part.data'
\set supplier_data_file :abs_srcdir '/data/supplier.data'
\set client_side_copy_command '\\copy customer FROM ' :'customer_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy customer_append FROM ' :'customer_1_data_file' ' with (delimiter '''|''', append_to_shard 360006);'
:client_side_copy_command
\set client_side_copy_command '\\copy nation FROM ' :'nation_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy part FROM ' :'part_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy part_append FROM ' :'part_data_file' ' with (delimiter '''|''', append_to_shard 360009);'
:client_side_copy_command
\set client_side_copy_command '\\copy supplier FROM ' :'supplier_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy supplier_single_shard FROM ' :'supplier_data_file' ' with delimiter '''|''';'
:client_side_copy_command
