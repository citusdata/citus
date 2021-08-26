CREATE TABLESPACE test_tablespace LOCATION '/home/talha/citus/src/test/regress/data/ts0';
\c - - - :worker_1_port
CREATE TABLESPACE test_tablespace LOCATION '/home/talha/citus/src/test/regress/data/ts1';
\c - - - :worker_2_port
CREATE TABLESPACE test_tablespace LOCATION '/home/talha/citus/src/test/regress/data/ts2';