SET search_path to "ch benchmarks";

-- Query 20
SELECT
    su_name,
    su_address
FROM
    supplier,
    nation
WHERE su_suppkey in
      (SELECT
           mod(s_i_id * s_w_id, 10000)
       FROM
           stock,
           order_line
       WHERE s_i_id IN
             (SELECT i_id
              FROM item
              WHERE i_data LIKE 'co%')
       AND ol_i_id = s_i_id
       AND ol_delivery_d > '2008-05-23 12:00:00' -- was 2010, but our order is in 2008
       GROUP BY s_i_id, s_w_id, s_quantity
       HAVING   2*s_quantity > sum(ol_quantity))
  AND su_nationkey = n_nationkey
  AND n_name = 'Germany'
ORDER BY su_name;

-- Multiple subqueries are supported IN and a NOT IN when no repartition join
-- is necessary and the IN subquery returns unique results
select s_i_id
    from stock
    where
        s_i_id in (select i_id from item)
        AND s_i_id not in (select i_im_id from item);


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
from     stock
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
ORDER BY s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock s
group by s_i_id
having   (select true)
order by s_i_id;

select     s_i_id, sum(s_order_cnt) as ordercount
from     stock
where   s_order_cnt > (select sum(s_order_cnt) * .005 as where_query from stock)
group by s_i_id
having   sum(s_order_cnt) > (select max(s_order_cnt) - 3 as having_query from stock)
order by s_i_id;

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
