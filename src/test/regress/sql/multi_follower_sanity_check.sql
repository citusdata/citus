-- check that the nodes are all in read-only mode and rejecting write queries

\c - - - :follower_master_port
CREATE TABLE tab (a int);
\c - - - :follower_worker_1_port
CREATE TABLE tab (a int);
\c - - - :follower_worker_2_port
CREATE TABLE tab (a int);
