--
-- MULTI_LOAD_DATA
--

\set lineitem_1_data_file :abs_srcdir '/data/lineitem.1.data'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem.2.data'
COPY lineitem FROM :'lineitem_1_data_file' with delimiter '|';
COPY lineitem FROM :'lineitem_2_data_file' with delimiter '|';

\set orders_1_data_file :abs_srcdir '/data/orders.1.data'
\set orders_2_data_file :abs_srcdir '/data/orders.2.data'
COPY orders FROM :'orders_1_data_file' with delimiter '|';
COPY orders FROM :'orders_2_data_file' with delimiter '|';

COPY orders_reference FROM :'orders_1_data_file' with delimiter '|';
COPY orders_reference FROM :'orders_2_data_file' with delimiter '|';

\set customer_1_data_file :abs_srcdir '/data/customer.1.data'
\set nation_data_file :abs_srcdir '/data/nation.data'
\set part_data_file :abs_srcdir '/data/part.data'
\set supplier_data_file :abs_srcdir '/data/supplier.data'
COPY customer FROM :'customer_1_data_file' with delimiter '|';
COPY customer_append FROM :'customer_1_data_file' with (delimiter '|', append_to_shard 360006);
COPY nation FROM :'nation_data_file' with delimiter '|';
COPY part FROM :'part_data_file' with delimiter '|';
COPY part_append FROM :'part_data_file' with (delimiter '|', append_to_shard 360009);
COPY supplier FROM :'supplier_data_file' with delimiter '|';
COPY supplier_single_shard FROM :'supplier_data_file' with delimiter '|';
