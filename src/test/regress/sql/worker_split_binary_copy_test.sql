CREATE SCHEMA worker_split_binary_copy_test;
SET search_path TO worker_split_binary_copy_test;
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 81060000;

-- Remove extra nodes added, otherwise GetLocalNodeId() does not bahave correctly.
SELECT citus_remove_node('localhost', 8887);
SELECT citus_remove_node('localhost', 9995);
SELECT citus_remove_node('localhost', 9992);
SELECT citus_remove_node('localhost', 9998);
SELECT citus_remove_node('localhost', 9997);
SELECT citus_remove_node('localhost', 8888);

-- BEGIN: Create distributed table and insert data.
CREATE TABLE worker_split_binary_copy_test.shard_to_split_copy (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
SELECT create_distributed_table('shard_to_split_copy', 'l_orderkey');

\COPY shard_to_split_copy FROM STDIN WITH DELIMITER '|'
99|87114|4639|1|10|11011.10|0.02|0.01|A|F|1994-05-18|1994-06-03|1994-05-23|COLLECT COD|RAIL|kages. requ
99|123766|3767|2|5|8948.80|0.02|0.07|R|F|1994-05-06|1994-05-28|1994-05-20|TAKE BACK RETURN|RAIL|ests cajole fluffily waters. blithe
99|134082|1622|3|42|46875.36|0.02|0.02|A|F|1994-04-19|1994-05-18|1994-04-20|NONE|RAIL|kages are fluffily furiously ir
99|108338|849|4|36|48467.88|0.09|0.02|A|F|1994-07-04|1994-04-17|1994-07-30|DELIVER IN PERSON|AIR|slyly. slyly e
100|62029|2030|1|28|27748.56|0.04|0.05|N|O|1998-05-08|1998-05-13|1998-06-07|COLLECT COD|TRUCK|sts haggle. slowl
100|115979|8491|2|22|43889.34|0.00|0.07|N|O|1998-06-24|1998-04-12|1998-06-29|DELIVER IN PERSON|SHIP|nto beans alongside of the fi
100|46150|8655|3|46|50422.90|0.03|0.04|N|O|1998-05-02|1998-04-10|1998-05-22|TAKE BACK RETURN|SHIP|ular accounts. even
100|38024|3031|4|14|13468.28|0.06|0.03|N|O|1998-05-22|1998-05-01|1998-06-03|COLLECT COD|MAIL|y. furiously ironic ideas gr
100|53439|955|5|37|51519.91|0.05|0.00|N|O|1998-03-06|1998-04-16|1998-03-31|TAKE BACK RETURN|TRUCK|nd the quickly s
101|118282|5816|1|49|63713.72|0.10|0.00|N|O|1996-06-21|1996-05-27|1996-06-29|DELIVER IN PERSON|REG AIR|ts
101|163334|883|2|36|50303.88|0.00|0.01|N|O|1996-05-19|1996-05-01|1996-06-04|DELIVER IN PERSON|AIR|tes. blithely pending dolphins x-ray f
101|138418|5958|3|12|17476.92|0.06|0.02|N|O|1996-03-29|1996-04-20|1996-04-12|COLLECT COD|MAIL|. quickly regular
102|88914|3931|1|37|70407.67|0.06|0.00|N|O|1997-07-24|1997-08-02|1997-08-07|TAKE BACK RETURN|SHIP|ully across the ideas. final deposit
102|169238|6787|2|34|44445.82|0.03|0.08|N|O|1997-08-09|1997-07-28|1997-08-26|TAKE BACK RETURN|SHIP|eposits cajole across
102|182321|4840|3|25|35083.00|0.01|0.01|N|O|1997-07-31|1997-07-24|1997-08-17|NONE|RAIL|bits. ironic accoun
102|61158|8677|4|15|16787.25|0.07|0.07|N|O|1997-06-02|1997-07-13|1997-06-04|DELIVER IN PERSON|SHIP|final packages. carefully even excu
103|194658|2216|1|6|10515.90|0.03|0.05|N|O|1996-10-11|1996-07-25|1996-10-28|NONE|FOB|cajole. carefully ex
103|10426|2928|2|37|49447.54|0.02|0.07|N|O|1996-09-17|1996-07-27|1996-09-20|TAKE BACK RETURN|MAIL|ies. quickly ironic requests use blithely
103|28431|8432|3|23|31266.89|0.01|0.04|N|O|1996-09-11|1996-09-18|1996-09-26|NONE|FOB|ironic accou
103|29022|4027|4|32|30432.64|0.01|0.07|N|O|1996-07-30|1996-08-06|1996-08-04|NONE|RAIL|kages doze. special, regular deposit
-1995148554|112942|2943|1|9|17594.46|0.04|0.04|N|O|1996-08-03|1996-05-31|1996-08-04|DELIVER IN PERSON|TRUCK|c realms print carefully car
-1686493264|15110|113|5|2|2050.22|0.03|0.08|R|F|1994-04-26|1994-03-15|1994-05-15|TAKE BACK RETURN|MAIL|e final, regular requests. carefully
\.

-- END: Create distributed table and insert data.

-- BEGIN: Switch to Worker1, Create target shards in worker for local 2-way split copy.
\c - - - :worker_1_port
CREATE TABLE worker_split_binary_copy_test.shard_to_split_copy_81060015 (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
CREATE TABLE worker_split_binary_copy_test.shard_to_split_copy_81060016 (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
-- End: Switch to Worker1, Create target shards in worker for local 2-way split copy.

-- BEGIN: Switch to Worker2, Create target shards in worker for remote 2-way split copy.
\c - - - :worker_2_port
CREATE TABLE worker_split_binary_copy_test.shard_to_split_copy_81060015 (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
CREATE TABLE worker_split_binary_copy_test.shard_to_split_copy_81060016 (
	l_orderkey bigint not null,
	l_partkey integer not null,
	l_suppkey integer not null,
	l_linenumber integer not null,
	l_quantity decimal(15, 2) not null,
	l_extendedprice decimal(15, 2) not null,
	l_discount decimal(15, 2) not null,
	l_tax decimal(15, 2) not null,
	l_returnflag char(1) not null,
	l_linestatus char(1) not null,
	l_shipdate date not null,
	l_commitdate date not null,
	l_receiptdate date not null,
	l_shipinstruct char(25) not null,
	l_shipmode char(10) not null,
	l_comment varchar(44) not null);
-- End: Switch to Worker2, Create target shards in worker for remote 2-way split copy.

-- BEGIN: List row count for source shard and targets shard in Worker1.
\c - - - :worker_1_port
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060000;
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060015;
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060016;
-- END: List row count for source shard and targets shard in Worker1.

-- BEGIN: List row count for target shard in Worker2.
\c - - - :worker_2_port
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060015;
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060016;
-- END: List row count for targets shard in Worker2.

-- BEGIN: Set worker_1_node and worker_2_node
\c - - - :worker_1_port
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
-- END: Set worker_1_node and worker_2_node

-- BEGIN: Trigger 2-way local shard split copy.
-- Ensure we will perform binary copy.
SET citus.enable_binary_protocol = true;

SELECT * from worker_split_copy(
    81060000, -- source shard id to copy
    'l_orderkey',
    ARRAY[
         -- split copy info for split children 1
        ROW(81060015, -- destination shard id
             -2147483648, -- split range begin
            1073741823, --split range end
            :worker_1_node)::pg_catalog.split_copy_info,
        -- split copy info for split children 2
        ROW(81060016,  --destination shard id
            1073741824, --split range begin
            2147483647, --split range end
            :worker_1_node)::pg_catalog.split_copy_info
        ]
    );
-- END: Trigger 2-way local shard split copy.

-- BEGIN: Trigger 2-way remote shard split copy.
SELECT * from worker_split_copy(
    81060000, -- source shard id to copy
    'l_orderkey',
    ARRAY[
         -- split copy info for split children 1
        ROW(81060015, -- destination shard id
             -2147483648, -- split range begin
            1073741823, --split range end
            :worker_2_node)::pg_catalog.split_copy_info,
        -- split copy info for split children 2
        ROW(81060016,  --destination shard id
            1073741824, --split range begin
            2147483647, --split range end
            :worker_2_node)::pg_catalog.split_copy_info
        ]
    );
-- END: Trigger 2-way remote shard split copy.

-- BEGIN: List updated row count for local targets shard.
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060015;
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060016;
-- END: List updated row count for local targets shard.

-- BEGIN: List updated row count for remote targets shard.
\c - - - :worker_2_port
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060015;
SELECT COUNT(*) FROM worker_split_binary_copy_test.shard_to_split_copy_81060016;
-- END: List updated row count for remote targets shard.

-- BEGIN: CLEANUP.
\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA worker_split_binary_copy_test CASCADE;
-- END: CLEANUP.
