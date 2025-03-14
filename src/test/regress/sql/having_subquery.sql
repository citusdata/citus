-- Testing a having clause that could have been a where clause between a distributed table
-- and a reference table. This query was the cause for intermediate results not being
-- available during the replace of the planner for the master query with the standard
-- planner.
-- Since the having clause could have been a where clause the having clause on the grouping
-- on the coordinator is replaced with a Result node containing a One-time filter if the
-- having qual (one-time filter works because the query doesn't change with the tuples
-- returned from below).
SELECT count(*),
       o_orderstatus
FROM orders
GROUP BY 2
HAVING (
           SELECT count(*)
           FROM customer
       ) > 0;

-- lets pin the plan in the test as well
SELECT public.explain_with_pg17_initplan_format($Q$
EXPLAIN (COSTS OFF)
SELECT count(*),
       o_orderstatus
FROM orders
GROUP BY 2
HAVING (
           SELECT count(*)
           FROM customer
       ) > 0;
$Q$) as "QUERY PLAN";
