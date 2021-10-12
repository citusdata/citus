SET search_path to "ch benchmarks";
SET search_path to "ch benchmarks";

-- Query 1
SELECT
    ol_number,
    sum(ol_quantity) as sum_qty,
    sum(ol_amount) as sum_amount,
    avg(ol_quantity) as avg_qty,
    avg(ol_amount) as avg_amount,
    count(*) as count_order
FROM order_line
WHERE ol_delivery_d > '2007-01-02 00:00:00.000000'
GROUP BY ol_number
ORDER BY ol_number;

-- Query 2
SELECT
    su_suppkey,
    su_name,
    n_name,
    i_id,
    i_name,
    su_address,
    su_phone,
    su_comment
FROM
    item,
    supplier,
    stock,
    nation,
    region,
    (SELECT
         s_i_id AS m_i_id,
         min(s_quantity) as m_s_quantity
     FROM
         stock,
         supplier,
         nation,
         region
     WHERE mod((s_w_id*s_i_id),10000)=su_suppkey
       AND su_nationkey=n_nationkey
       AND n_regionkey=r_regionkey
       AND r_name LIKE 'Europ%'
     GROUP BY s_i_id) m
WHERE i_id = s_i_id
  AND mod((s_w_id * s_i_id), 10000) = su_suppkey
  AND su_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND i_data LIKE '%b'
  AND r_name LIKE 'Europ%'
  AND i_id = m_i_id
  AND s_quantity = m_s_quantity
ORDER BY
    n_name,
    su_name,
    i_id;

-- Query 3
SELECT
    ol_o_id,
    ol_w_id,
    ol_d_id,
    sum(ol_amount) AS revenue,
    o_entry_d
FROM
    customer,
    new_order,
    oorder,
    order_line
WHERE c_state LIKE 'C%' -- used to ba A%, but C% works with our small data
  AND c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND no_w_id = o_w_id
  AND no_d_id = o_d_id
  AND no_o_id = o_id
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND o_entry_d > '2007-01-02 00:00:00.000000'
GROUP BY
    ol_o_id,
    ol_w_id,
    ol_d_id,
    o_entry_d
ORDER BY
    revenue DESC,
    o_entry_d;

-- Query 4
SELECT
    o_ol_cnt,
    count(*) as order_count
FROM
    oorder
WHERE o_entry_d >= '2007-01-02 00:00:00.000000'
  AND o_entry_d < '2012-01-02 00:00:00.000000'
  AND exists (SELECT *
              FROM order_line
              WHERE o_id = ol_o_id
                AND o_w_id = ol_w_id
                AND o_d_id = ol_d_id
                AND ol_delivery_d >= o_entry_d)
GROUP BY o_ol_cnt
ORDER BY o_ol_cnt;

-- Query 5
SELECT
    n_name,
    sum(ol_amount) AS revenue
FROM
    customer,
    oorder,
    order_line,
    stock,
    supplier,
    nation,
    region
WHERE c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND ol_o_id = o_id
  AND ol_w_id = o_w_id
  AND ol_d_id=o_d_id
  AND ol_w_id = s_w_id
  AND ol_i_id = s_i_id
  AND mod((s_w_id * s_i_id),10000) = su_suppkey
-- our dataset does not have the supplier in the same nation as the customer causing this
-- join to filter out all the data. We verify later on that we can actually perform an
-- ascii(substr(c_state,1,1)) == reference table column join later on so it should not
-- matter we skip this filter here.
--AND ascii(substr(c_state,1,1)) = su_nationkey
  AND su_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'Europe'
  AND o_entry_d >= '2007-01-02 00:00:00.000000'
GROUP BY n_name
ORDER BY revenue DESC;

-- Query 6
SELECT
    sum(ol_amount) AS revenue
FROM order_line
WHERE ol_delivery_d >= '1999-01-01 00:00:00.000000'
  AND ol_delivery_d < '2020-01-01 00:00:00.000000'
  AND ol_quantity BETWEEN 1 AND 100000;

-- Query 7
SELECT
    su_nationkey as supp_nation,
    substr(c_state,1,1) as cust_nation,
    extract(year from o_entry_d) as l_year,
    sum(ol_amount) as revenue
FROM
    supplier,
    stock,
    order_line,
    oorder,
    customer,
    nation n1,
    nation n2
WHERE ol_supply_w_id = s_w_id
  AND ol_i_id = s_i_id
  AND mod((s_w_id * s_i_id), 10000) = su_suppkey
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND su_nationkey = n1.n_nationkey
  AND ascii(substr(c_state,1,1)) = n2.n_nationkey
  AND (
         (n1.n_name = 'Germany' AND n2.n_name = 'Cambodia')
      OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany')
      )
  AND ol_delivery_d BETWEEN '2007-01-02 00:00:00.000000' AND '2012-01-02 00:00:00.000000'
GROUP BY
    su_nationkey,
    substr(c_state,1,1),
    extract(year from o_entry_d)
ORDER BY
    su_nationkey,
    cust_nation,
    l_year;
