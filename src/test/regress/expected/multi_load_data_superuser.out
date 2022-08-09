\set lineitem_1_data_file :abs_srcdir '/data/lineitem.1.data'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem.2.data'
\set orders_1_data_file :abs_srcdir '/data/orders.1.data'
\set orders_2_data_file :abs_srcdir '/data/orders.2.data'
\set client_side_copy_command '\\copy lineitem_hash_part FROM ' :'lineitem_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy lineitem_hash_part FROM ' :'lineitem_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders_hash_part FROM ' :'orders_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy orders_hash_part FROM ' :'orders_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command
