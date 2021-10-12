SET search_path to "ch benchmarks";

-- Query 8
SELECT
    extract(year from o_entry_d) as l_year,
    sum(case when n2.n_name = 'Germany' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
FROM
    item,
    supplier,
    stock,
    order_line,
    oorder,
    customer,
    nation n1,
    nation n2,
    region
WHERE i_id = s_i_id
  AND ol_i_id = s_i_id
  AND ol_supply_w_id = s_w_id
  AND mod((s_w_id * s_i_id),10000) = su_suppkey
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND n1.n_nationkey = ascii(substr(c_state,1,1))
  AND n1.n_regionkey = r_regionkey
  AND ol_i_id < 1000
  AND r_name = 'Europe'
  AND su_nationkey = n2.n_nationkey
  AND o_entry_d BETWEEN '2007-01-02 00:00:00.000000' AND '2012-01-02 00:00:00.000000'
  AND i_data LIKE '%b'
  AND i_id = ol_i_id
GROUP BY extract(YEAR FROM o_entry_d)
ORDER BY l_year;

-- Query 9
SELECT
    n_name,
    extract(year from o_entry_d) as l_year,
    sum(ol_amount) as sum_profit
FROM
    item,
    stock,
    supplier,
    order_line,
    oorder,
    nation
WHERE ol_i_id = s_i_id
  AND ol_supply_w_id = s_w_id
  AND mod((s_w_id * s_i_id), 10000) = su_suppkey
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND ol_i_id = i_id
  AND su_nationkey = n_nationkey
  AND i_data LIKE '%b' -- this used to be %BB but that will not work with our small dataset
GROUP BY
    n_name,
    extract(YEAR FROM o_entry_d)
ORDER BY
    n_name,
    l_year DESC;

-- Query 10
SELECT
    c_id,
    c_last,
    sum(ol_amount) AS revenue,
    c_city,
    c_phone,
    n_name
FROM
    customer,
    oorder,
    order_line,
    nation
WHERE c_id = o_c_id
  AND c_w_id = o_w_id
  AND c_d_id = o_d_id
  AND ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND o_entry_d >= '2007-01-02 00:00:00.000000'
  AND o_entry_d <= ol_delivery_d
  AND n_nationkey = ascii(substr(c_state,1,1))
GROUP BY
    c_id,
    c_last,
    c_city,
    c_phone,
    n_name
ORDER BY revenue DESC;

-- Query 11
SELECT
    s_i_id,
    sum(s_order_cnt) AS ordercount
FROM
    stock,
    supplier,
    nation
WHERE mod((s_w_id * s_i_id),10000) = su_suppkey
  AND su_nationkey = n_nationkey
  AND n_name = 'Germany'
GROUP BY s_i_id
HAVING sum(s_order_cnt) >
         (SELECT sum(s_order_cnt) * .005
          FROM
              stock,
              supplier,
              nation
          WHERE mod((s_w_id * s_i_id),10000) = su_suppkey
            AND su_nationkey = n_nationkey
            AND n_name = 'Germany')
ORDER BY ordercount DESC;

-- Query 12
SELECT
    o_ol_cnt,
    sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
    sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
FROM
    oorder,
    order_line
WHERE ol_w_id = o_w_id
  AND ol_d_id = o_d_id
  AND ol_o_id = o_id
  AND o_entry_d <= ol_delivery_d
  AND ol_delivery_d < '2020-01-01 00:00:00.000000'
GROUP BY o_ol_cnt
ORDER BY o_ol_cnt;

-- Query 13
SELECT
    c_count,
    count(*) AS custdist
FROM (SELECT
          c_id,
          count(o_id)
      FROM customer
               LEFT OUTER JOIN oorder ON (
                  c_w_id = o_w_id
              AND c_d_id = o_d_id
              AND c_id = o_c_id
              AND o_carrier_id > 8)
      GROUP BY c_id) AS c_orders (c_id, c_count)
GROUP BY c_count
ORDER BY
    custdist DESC,
    c_count DESC;

-- Query 14
SELECT
    100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1+sum(ol_amount)) AS promo_revenue
FROM
    order_line,
    item
WHERE ol_i_id = i_id
  AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
  AND ol_delivery_d < '2020-01-02 00:00:00.000000';

-- Query 15
WITH revenue (supplier_no, total_revenue) AS (
    SELECT
        mod((s_w_id * s_i_id),10000) AS supplier_no,
        sum(ol_amount) AS total_revenue
    FROM
        order_line,
        stock
    WHERE ol_i_id = s_i_id
      AND ol_supply_w_id = s_w_id
      AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
    GROUP BY mod((s_w_id * s_i_id),10000))
SELECT
    su_suppkey,
    su_name,
    su_address,
    su_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE su_suppkey = supplier_no
  AND total_revenue = (SELECT max(total_revenue) FROM revenue)
ORDER BY su_suppkey;
