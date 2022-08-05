\set test_tablespace :abs_srcdir '/tmp_check/ts0'
CREATE TABLESPACE test_tablespace LOCATION :'test_tablespace';
\c - - - :worker_1_port
\set test_tablespace :abs_srcdir '/tmp_check/ts1'
CREATE TABLESPACE test_tablespace LOCATION :'test_tablespace';
\c - - - :worker_2_port
\set test_tablespace :abs_srcdir '/tmp_check/ts2'
CREATE TABLESPACE test_tablespace LOCATION :'test_tablespace';
