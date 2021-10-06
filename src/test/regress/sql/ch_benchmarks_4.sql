SET search_path to "ch benchmarks";

--Q16
SELECT
    i_name,
    substr(i_data, 1, 3) AS brand,
    i_price,
    count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt
FROM
    stock,
    item
WHERE i_id = s_i_id
  AND i_data NOT LIKE 'zz%'
  AND (mod((s_w_id * s_i_id),10000) NOT IN
       (SELECT su_suppkey
        FROM supplier
        WHERE su_comment LIKE '%bad%'))
GROUP BY
    i_name,
    substr(i_data, 1, 3),
    i_price
ORDER BY supplier_cnt DESC;

--Q17
SELECT
       sum(ol_amount) / 2.0 AS avg_yearly
FROM
    order_line,
    (SELECT
         i_id,
         avg(ol_quantity) AS a
     FROM
         item,
         order_line
     WHERE i_data LIKE '%b'
       AND ol_i_id = i_id
     GROUP BY i_id) t
WHERE ol_i_id = t.i_id;
-- this filter was at the end causing the dataset to be empty. it should not have any
-- influence on how the query gets planned so I removed the clause
--AND ol_quantity < t.a;

-- Query 18
SELECT
    c_last,
    c_id o_id,
    o_entry_d,
    o_ol_cnt,
    sum(ol_amount)
FROM
    customer,
    oorder,
    order_line
WHERE c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
GROUP BY
    o_id,
    o_w_id,
    o_d_id,
    c_id,
    c_last,
    o_entry_d,
    o_ol_cnt
HAVING sum(ol_amount) > 5 -- was 200, but thats too big for the dataset
ORDER BY
    sum(ol_amount) DESC,
    o_entry_d;

-- Query 19
SELECT
    sum(ol_amount) AS revenue
FROM
    order_line,
     item
WHERE (     ol_i_id = i_id
        AND i_data LIKE '%a'
        AND ol_quantity >= 1
        AND ol_quantity <= 10
        AND i_price BETWEEN 1 AND 400000
        AND ol_w_id IN (1,2,3))
   OR (     ol_i_id = i_id
        AND i_data LIKE '%b'
        AND ol_quantity >= 1
        AND ol_quantity <= 10
        AND i_price BETWEEN 1 AND 400000
        AND ol_w_id IN (1,2,4))
   OR (     ol_i_id = i_id
        AND i_data LIKE '%c'
        AND ol_quantity >= 1
        AND ol_quantity <= 10
        AND i_price BETWEEN 1 AND 400000
        AND ol_w_id IN (1,5,3));

select   su_name, su_address
from     supplier, nation
where    su_suppkey in
        (select  mod(s_i_id * s_w_id, 10000)
        from     stock, order_line
        where    s_i_id in
                (select i_id
                 from item)
             and ol_i_id=s_i_id
             and ol_delivery_d > '2010-05-23 12:00:00'
        group by s_i_id, s_w_id, s_quantity
        having   2*s_quantity > sum(ol_quantity))
     and su_nationkey = n_nationkey
     and n_name = 'GERMANY'
order by su_name;
