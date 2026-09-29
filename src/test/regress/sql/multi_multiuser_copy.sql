--
-- MULTI_MULTIUSER_COPY
--

-- Make this test re-runnable within the same cluster.
SET client_min_messages TO WARNING;
DROP TABLE IF EXISTS customer_copy_hash;
RESET client_min_messages;

-- Create a new hash-partitioned table into which to COPY
CREATE TABLE customer_copy_hash (
        c_custkey integer,
        c_name varchar(25) not null,
        c_address varchar(40),
        c_nationkey integer,
        c_phone char(15),
        c_acctbal decimal(15,2),
        c_mktsegment char(10),
        c_comment varchar(117),
		primary key (c_custkey));
SELECT create_distributed_table('customer_copy_hash', 'c_custkey', 'hash');
GRANT ALL ON TABLE customer_copy_hash TO full_access;
GRANT SELECT ON TABLE customer_copy_hash TO read_access;

-- COPY FROM as superuser
COPY customer_copy_hash (c_custkey,c_name) FROM STDIN;
1	customer1
\.

-- COPY FROM as user with ALL access
SET ROLE full_access;
COPY customer_copy_hash (c_custkey,c_name) FROM STDIN;
2	customer2
\.
RESET ROLE;

-- COPY FROM as user with SELECT access, should fail
SET ROLE read_access;
\copy customer_copy_hash (c_custkey,c_name) FROM PROGRAM 'true'
RESET ROLE;

-- COPY FROM as user with no access, should fail
SET ROLE no_access;
\copy customer_copy_hash (c_custkey,c_name) FROM PROGRAM 'true'
RESET ROLE;


-- COPY TO as superuser
COPY (SELECT * FROM customer_copy_hash ORDER BY 1) TO STDOUT;

SET ROLE full_access;
COPY (SELECT * FROM customer_copy_hash ORDER BY 1) TO STDOUT;
RESET ROLE;

-- COPY FROM as user with SELECT access, should work
SET ROLE read_access;
COPY (SELECT * FROM customer_copy_hash ORDER BY 1) TO STDOUT;
RESET ROLE;

-- COPY FROM as user with no access, should fail
SET ROLE no_access;
COPY (SELECT * FROM customer_copy_hash ORDER BY 1) TO STDOUT;
RESET ROLE;
