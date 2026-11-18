CREATE SCHEMA "ch benchmarks";
SET search_path to "ch benchmarks";
GRANT ALL ON SCHEMA "ch benchmarks" TO regularuser;


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
CREATE TABLE new_order (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    PRIMARY KEY (no_w_id,no_d_id,no_o_id)
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
CREATE TABLE oorder (
    o_w_id int NOT NULL,
    o_d_id int NOT NULL,
    o_id int NOT NULL,
    o_c_id int NOT NULL,
    o_carrier_id int DEFAULT NULL,
    o_ol_cnt decimal(2,0) NOT NULL,
    o_all_local decimal(1,0) NOT NULL,
    o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (o_w_id,o_d_id,o_id),
    UNIQUE (o_w_id,o_d_id,o_c_id,o_id)
);
CREATE TABLE history (
    h_c_id int NOT NULL,
    h_c_d_id int NOT NULL,
    h_c_w_id int NOT NULL,
    h_d_id int NOT NULL,
    h_w_id int NOT NULL,
    h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount decimal(6,2) NOT NULL,
    h_data varchar(24) NOT NULL
);
CREATE TABLE customer (
    c_w_id int NOT NULL,
    c_d_id int NOT NULL,
    c_id int NOT NULL,
    c_discount decimal(4,4) NOT NULL,
    c_credit char(2) NOT NULL,
    c_last varchar(16) NOT NULL,
    c_first varchar(16) NOT NULL,
    c_credit_lim decimal(12,2) NOT NULL,
    c_balance decimal(12,2) NOT NULL,
    c_ytd_payment float NOT NULL,
    c_payment_cnt int NOT NULL,
    c_delivery_cnt int NOT NULL,
    c_street_1 varchar(20) NOT NULL,
    c_street_2 varchar(20) NOT NULL,
    c_city varchar(20) NOT NULL,
    c_state char(2) NOT NULL,
    c_zip char(9) NOT NULL,
    c_phone char(16) NOT NULL,
    c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle char(2) NOT NULL,
    c_data varchar(500) NOT NULL,
    PRIMARY KEY (c_w_id,c_d_id,c_id)
);
CREATE TABLE district (
    d_w_id int NOT NULL,
    d_id int NOT NULL,
    d_ytd decimal(12,2) NOT NULL,
    d_tax decimal(4,4) NOT NULL,
    d_next_o_id int NOT NULL,
    d_name varchar(10) NOT NULL,
    d_street_1 varchar(20) NOT NULL,
    d_street_2 varchar(20) NOT NULL,
    d_city varchar(20) NOT NULL,
    d_state char(2) NOT NULL,
    d_zip char(9) NOT NULL,
    PRIMARY KEY (d_w_id,d_id)
);
CREATE TABLE item (
    i_id int NOT NULL,
    i_name varchar(24) NOT NULL,
    i_price decimal(5,2) NOT NULL,
    i_data varchar(50) NOT NULL,
    i_im_id int NOT NULL,
    PRIMARY KEY (i_id)
);
CREATE TABLE warehouse (
    w_id int NOT NULL,
    w_ytd decimal(12,2) NOT NULL,
    w_tax decimal(4,4) NOT NULL,
    w_name varchar(10) NOT NULL,
    w_street_1 varchar(20) NOT NULL,
    w_street_2 varchar(20) NOT NULL,
    w_city varchar(20) NOT NULL,
    w_state char(2) NOT NULL,
    w_zip char(9) NOT NULL,
    PRIMARY KEY (w_id)
);
CREATE TABLE region (
    r_regionkey int not null,
    r_name char(55) not null,
    r_comment char(152) not null,
    PRIMARY KEY ( r_regionkey )
);
CREATE TABLE nation (
    n_nationkey int not null,
    n_name char(25) not null,
    n_regionkey int not null,
    n_comment char(152) not null,
    PRIMARY KEY ( n_nationkey )
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
SELECT create_distributed_table('new_order','no_w_id');
SELECT create_distributed_table('stock','s_w_id');
SELECT create_distributed_table('oorder','o_w_id');
SELECT create_distributed_table('history','h_w_id');
SELECT create_distributed_table('customer','c_w_id');
SELECT create_distributed_table('district','d_w_id');
SELECT create_distributed_table('warehouse','w_id');
SELECT create_reference_table('item');
SELECT create_reference_table('region');
SELECT create_reference_table('nation');
SELECT create_reference_table('supplier');

TRUNCATE order_line, new_order, stock, oorder, history, customer, district, warehouse, item, region, nation, supplier; -- for easy copy in development
INSERT INTO supplier SELECT c, 'abc', 'def', c, 'ghi', c, 'jkl' FROM generate_series(0,10) AS c;
INSERT INTO new_order SELECT c, c, c FROM generate_series(0,10) AS c;
INSERT INTO stock SELECT c,c,c,c,c,c, 'abc','abc','abc','abc','abc','abc','abc','abc','abc','abc','abc' FROM generate_series(1,3) AS c;
INSERT INTO stock SELECT c, 5000,c,c,c,c, 'abc','abc','abc','abc','abc','abc','abc','abc','abc','abc','abc' FROM generate_series(1,3) AS c; -- mod(2*5000,10000) == 0
INSERT INTO order_line SELECT c, c, c, c, c, '2008-10-17 00:00:00.000000', c, c, c, 'abc' FROM generate_series(0,10) AS c;
INSERT INTO oorder SELECT c, c, c, c, c, 1, 1, '2008-10-17 00:00:00.000000' FROM generate_series(0,10) AS c;
INSERT INTO customer SELECT c, c, c, 0, 'XX', 'John', 'Doe', 1000, 0, 0, c, c, 'Name', 'Street', 'Some City', 'CA', '12345', '+1 000 0000000', '2007-01-02 00:00:00.000000', 'NA', 'nothing special' FROM generate_series(0,10) AS c;
INSERT INTO item SELECT c, 'Keyboard', 50, 'co b', c FROM generate_series(0,10) AS c; --co% and %b filters all around
INSERT INTO region VALUES
    (1, 'Not Europe', 'Big'),
    (2, 'Europe', 'Big');
INSERT INTO nation VALUES
    (1, 'United States', 1, 'Also Big'),
    (4, 'The Netherlands', 2, 'Flat'),
    (9, 'Germany', 2, 'Germany must be in here for Q7'),
    (67, 'Cambodia', 2, 'I don''t understand how we got from California to Cambodia but I will take it, it also is not in Europe, but we need it to be for Q8');
