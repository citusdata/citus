--
-- MULTI_LOAD_DATA
--

\set lineitem_1_data_file :abs_srcdir '/data/lineitem_1_data_file'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem_2_data_file'
COPY lineitem FROM :'lineitem_1_data_file' with delimiter '|'
COPY lineitem FROM :'lineitem_2_data_file' with delimiter '|'

\set orders_1_data_file :abs_srcdir '/data/orders_1_data_file'
\set orders_2_data_file :abs_srcdir '/data/orders_2_data_file'
COPY orders FROM :'orders_1_data_file' with delimiter '|'
COPY orders FROM :'orders_2_data_file' with delimiter '|'

COPY orders_reference FROM :'orders_1_data_file' with delimiter '|'
COPY orders_reference FROM :'orders_2_data_file' with delimiter '|'

\set customer_1_data_file :abs_srcdir '/data/customer_1_data_file'
\set nation_data_file :abs_srcdir '/data/nation_data_file'
\set part_data_file :abs_srcdir '/data/part_data_file'
\set supplier_data_file :abs_srcdir '/data/supplier_data_file'
COPY customer FROM :'customer_1_data_file' with delimiter '|'
COPY customer_append FROM :'customer_1_data_file' with (delimiter '|', append_to_shard 360006)
COPY nation FROM :'nation_data_file' with delimiter '|'
COPY part FROM :'part_data_file' with delimiter '|'
COPY part_append FROM :'part_data_file' with (delimiter '|', append_to_shard 360009)
COPY supplier FROM :'supplier_data_file' with delimiter '|'
COPY supplier_single_shard FROM :'supplier_data_file' with delimiter '|'
