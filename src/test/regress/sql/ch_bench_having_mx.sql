ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1640000;
SET citus.replication_model TO streaming;
SET citus.shard_replication_factor to 1;
SET citus.shard_count to 4;

CREATE SCHEMA ch_bench_having;
SET search_path = ch_bench_having;

CREATE TABLE stock (
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_order_cnt int NOT NULL
);

SELECT create_distributed_table('stock','s_w_id');

\c - - :public_worker_1_host :worker_1_port
SET search_path = ch_bench_having;
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


\c - - :master_host :master_port
SET citus.replication_model TO streaming;
SET citus.shard_replication_factor to 1;
SET citus.shard_count to 4;

SET search_path = ch_bench_having, public;
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

insert into stock VALUES
(1, 33, 1, 1, 1, 1, '', '','','','','','','','','',''),
(33, 1, 1, 1, 1, 1, '', '','','','','','','','','',''),
(32, 1, 1, 1, 1, 1, '', '','','','','','','','','','');

SELECT create_distributed_table('stock','s_w_id');

\c - - :public_worker_1_host :worker_1_port
SET search_path = ch_bench_having, public;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock, supplier_mx, nation_mx
where     mod((s_w_id * s_i_id),10000) = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'GERMANY'
group by s_i_id
having   sum(s_order_cnt) >
        (select sum(s_order_cnt) * .005
        from stock, supplier_mx, nation_mx
        where mod((s_w_id * s_i_id),10000) = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY')
order by ordercount desc;

insert into stock VALUES
(10033, 1, 1, 1, 100000, 1, '', '','','','','','','','','','');

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock, supplier_mx, nation_mx
where     mod((s_w_id * s_i_id),10000) = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'GERMANY'
group by s_i_id
having   sum(s_order_cnt) >
        (select sum(s_order_cnt) * .005
        from stock, supplier_mx, nation_mx
        where mod((s_w_id * s_i_id),10000) = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY')
order by ordercount desc;

\c - - :master_host :master_port
BEGIN;
SET LOCAL client_min_messages TO WARNING;
DROP SCHEMA ch_bench_having CASCADE;
COMMIT;
