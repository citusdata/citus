SET citus.next_shard_id TO 1640000;
CREATE SCHEMA ch_bench_having;
SET search_path = ch_bench_having;

CREATE TABLE stock (
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_order_cnt int NOT NULL
);

SELECT create_distributed_table('stock','s_w_id');

explain (costs false, summary false, timing false)
select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;

explain (costs false, summary false, timing false)
select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;

explain (costs false, summary false, timing false)
select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock);


explain select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true)
order by s_i_id;

explain select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true);

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;

INSERT INTO stock SELECT c, c, c FROM generate_series(1, 5) as c;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;


select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   (select true)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   (select false)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true)
order by s_i_id;


select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select false)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true)
order by s_i_id;


-- We don't support correlated subqueries in having
select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   (select max(s_order_cnt) > 2 as having_query from stock where s_i_id = s.s_i_id)
order by s_i_id;

-- We don't support correlated subqueries in having
select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select max(s_order_cnt) > 2 as having_query from stock where s_i_id = s.s_i_id)
order by s_i_id;


DROP TABLE stock;

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

create table supplier (
   su_suppkey int not null,
   su_name char(25) not null,
   su_address varchar(40) not null,
   su_nationkey int not null,
   su_phone char(15) not null,
   su_acctbal numeric(12,2) not null,
   su_comment char(101) not null,
   PRIMARY KEY ( su_suppkey )
);

create table nation (
   n_nationkey int not null,
   n_name char(25) not null,
   n_regionkey int not null,
   n_comment char(152) not null,
   PRIMARY KEY ( n_nationkey )
);

SELECT create_distributed_table('stock','s_w_id');
SELECT create_reference_table('nation');
SELECT create_reference_table('supplier');

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock, supplier, nation
where     mod((s_w_id * s_i_id),10000) = su_suppkey
     and su_nationkey = n_nationkey
     and n_name = 'Germany'
group by s_i_id
having   sum(s_order_cnt) >
        (select sum(s_order_cnt) * .005
        from stock, supplier, nation
        where mod((s_w_id * s_i_id),10000) = su_suppkey
        and su_nationkey = n_nationkey
        and n_name = 'Germany')
order by ordercount desc;

BEGIN;
SET LOCAL client_min_messages TO WARNING;
DROP SCHEMA ch_bench_having CASCADE;
COMMIT;
