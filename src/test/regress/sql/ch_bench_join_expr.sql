SET citus.next_shard_id TO 1650000;
CREATE SCHEMA ch_bench_join_expr;
SET search_path TO ch_bench_join_expr;

SET citus.enable_repartition_joins TO on;

CREATE TABLE order_line (
    ol_w_id int NOT NULL,
    ol_d_id int NOT NULL,
    ol_o_id int NOT NULL,
    ol_number int NOT NULL,
    ol_i_id int NOT NULL,
    ol_delivery_d timestamp NULL DEFAULT NULL,
    ol_amount decimal(6,2) NOT NULL,
    ol_supply_w_id int NOT NULL,
    ol_quantity decimal(2,0) NOT NULL,
    ol_dist_info char(24) NOT NULL,
    PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

CREATE TABLE stock (
    s_w_id int NOT NULL,
    s_i_id int NOT NULL,
    s_quantity decimal(4,0) NOT NULL,
    s_ytd decimal(8,2) NOT NULL,
    s_order_cnt int NOT NULL,
    s_remote_cnt int NOT NULL,
    s_data varchar(50) NOT NULL,
    s_dist_01 char(24) NOT NULL,
    s_dist_02 char(24) NOT NULL,
    s_dist_03 char(24) NOT NULL,
    s_dist_04 char(24) NOT NULL,
    s_dist_05 char(24) NOT NULL,
    s_dist_06 char(24) NOT NULL,
    s_dist_07 char(24) NOT NULL,
    s_dist_08 char(24) NOT NULL,
    s_dist_09 char(24) NOT NULL,
    s_dist_10 char(24) NOT NULL,
    PRIMARY KEY (s_w_id,s_i_id)
);

CREATE TABLE supplier (
    su_suppkey int not null,
    su_name char(25) not null,
    su_address varchar(40) not null,
    su_nationkey int not null,
    su_phone char(15) not null,
    su_acctbal numeric(12,2) not null,
    su_comment char(101) not null,
    PRIMARY KEY ( su_suppkey )
);

SELECT create_distributed_table('order_line','ol_w_id');
SELECT create_distributed_table('stock','s_w_id');
SELECT create_reference_table('supplier');

INSERT INTO supplier SELECT c, 'abc', 'def', c, 'ghi', c, 'jkl' FROM generate_series(0,10) AS c;
INSERT INTO stock SELECT c,c,c,c,c,c, 'abc','abc','abc','abc','abc','abc','abc','abc','abc','abc','abc' FROM generate_series(1,3) AS c;
INSERT INTO stock SELECT c, 5000,c,c,c,c, 'abc','abc','abc','abc','abc','abc','abc','abc','abc','abc','abc' FROM generate_series(1,3) AS c; -- mod(2*5000,10000) == 0
INSERT INTO order_line SELECT c, c, c, c, c, NULL, c, c, c, 'abc' FROM generate_series(0,10) AS c;

-- MVP for CH-BenCHmark queries 7/8/9 where a join with an expression is done between a distributed table and a reference table.
  SELECT su_suppkey, s_w_id, s_i_id, ol_supply_w_id
    FROM supplier, stock, order_line
   WHERE ol_supply_w_id = s_w_id
     AND mod((s_w_id * s_i_id),10000) = su_suppkey
ORDER BY 1,2,3,4;

SET client_min_messages TO WARNING;
DROP SCHEMA ch_bench_join_expr CASCADE;
