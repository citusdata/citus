--
-- MULTI_SINGLE_RELATION_SUBQUERY
--

-- This test checks that we are able to run selected set of distributed SQL subqueries.

SET citus.task_executor_type TO 'task-tracker';

select
    number_sum,
    count(*) as total,
    avg(total_count) avg_count
from
    (select
        l_suppkey,
        l_linestatus,
        sum(l_linenumber) as number_sum,
        count(*) as total_count
    from
        lineitem
    group by
        l_suppkey,
        l_linestatus) as distributed_table
where
    number_sum >= 10
group by
    number_sum
order by
    total desc,
    number_sum desc
limit 10;

select
    (l_suppkey / 100) as suppkey_bin,
    avg(total_count) avg_count
from
    (select
        l_suppkey,
        sum(l_linenumber) as number_sum,
        count(*) as total_count
    from
        lineitem
    group by
        l_suppkey,
        l_linestatus) as distributed_table
group by
    suppkey_bin
order by
    avg_count desc
limit 20;

select
    total,
    avg(avg_count) as total_avg_count
from
    (select
        number_sum,
        count(*) as total,
        avg(total_count) avg_count
    from
        (select
            l_suppkey,
            sum(l_linenumber) as number_sum,
            count(*) as total_count
        from
            lineitem
        where
            l_partkey > 100 and
            l_quantity > 2 and
            l_orderkey < 10000
        group by
            l_suppkey) as distributed_table
    where
        number_sum >= 10
    group by
        number_sum) as distributed_table_2
group by
    total
order by
    total;

-- Check that we support subquery even though group by clause is an expression
-- and it is not referred in the target list.

select
    avg(count)
from
    (select
        l_suppkey,
        count(*) as count
    from
        lineitem
    group by
        (l_orderkey/4)::int,
        l_suppkey )  as distributed_table;

-- Check that we don't support subqueries with limit.

select
    l_suppkey,
    sum(suppkey_count) as total_suppkey_count
from
    (select
        l_suppkey,
        count(*) as suppkey_count
    from
        lineitem
    group by
        l_suppkey
    order by
        l_suppkey
    limit 100) as distributed_table
group by
    l_suppkey;

-- Check that we don't support subqueries without aggregates.

select
    rounded_tax
from
    (select
        round(l_tax) as rounded_tax
    from
        lineitem
    group by
        l_tax) as distributed_table;

-- Check that we support subqueries with count(distinct).

select
    avg(different_shipment_days)
from
    (select
        count(distinct l_shipdate) as different_shipment_days
    from
        lineitem
    group by
        l_partkey) as distributed_table;

-- Check that if subquery is pulled, we don't error and run query properly.

SELECT max(l_suppkey) FROM
(
    SELECT
        l_suppkey
        FROM (
            SELECT
                l_suppkey,
                count(*)
            FROM
                lineitem_subquery
            WHERE
                l_orderkey < 20000
            GROUP BY
                l_suppkey) z
) y;

