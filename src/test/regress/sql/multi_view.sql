--
-- MULTI_VIEW
--

-- This file contains test cases for view support. It verifies various
-- Citus features: simple selects, aggregates, joins, outer joins
-- router queries, single row inserts, multi row inserts via insert
-- into select, multi row insert via copy commands.

SELECT count(*) FROM lineitem_hash_part;

SELECT count(*) FROM orders_hash_part;

-- create a view for priority orders
CREATE VIEW priority_orders AS SELECT * FROM orders_hash_part WHERE o_orderpriority < '3-MEDIUM';

-- aggregate pushdown
SELECT o_orderpriority, count(*)  FROM priority_orders GROUP BY 1 ORDER BY 2, 1;

SELECT o_orderpriority, count(*) FROM orders_hash_part  WHERE o_orderpriority < '3-MEDIUM' GROUP BY 1 ORDER BY 2,1;

-- filters
SELECT o_orderpriority, count(*) as all, count(*) FILTER (WHERE o_orderstatus ='F') as fullfilled  FROM priority_orders GROUP BY 1 ORDER BY 2, 1;

-- having
SELECT o_orderdate, count(*) from priority_orders group by 1 having (count(*) > 3)  order by 2 desc, 1 desc;

-- having with filters
SELECT o_orderdate, count(*) as all, count(*) FILTER(WHERE o_orderstatus = 'F') from priority_orders group by 1 having (count(*) > 3)  order by 2 desc, 1 desc;

-- limit
SELECT o_orderkey, o_totalprice from orders_hash_part order by 2 desc, 1 asc limit 5 ;

SELECT o_orderkey, o_totalprice from priority_orders order by 2 desc, 1 asc limit 1 ;

CREATE VIEW priority_lineitem AS SELECT li.* FROM lineitem_hash_part li JOIN priority_orders ON (l_orderkey = o_orderkey);

SELECT l_orderkey, count(*) FROM priority_lineitem GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5;

CREATE VIEW air_shipped_lineitems AS SELECT * FROM lineitem_hash_part WHERE l_shipmode = 'AIR';

-- join between view and table
SELECT count(*) FROM orders_hash_part join air_shipped_lineitems ON (o_orderkey = l_orderkey);

-- join between views
SELECT count(*) FROM priority_orders join air_shipped_lineitems ON (o_orderkey = l_orderkey);

-- count distinct on partition column is supported
SELECT count(distinct o_orderkey) FROM priority_orders join air_shipped_lineitems ON (o_orderkey = l_orderkey);

-- count distinct on non-partition column is supported
SELECT count(distinct o_orderpriority) FROM priority_orders join air_shipped_lineitems ON (o_orderkey = l_orderkey);

-- count distinct on partition column is supported on router queries
SELECT count(distinct o_orderkey) FROM priority_orders join air_shipped_lineitems
	ON (o_orderkey = l_orderkey)
	WHERE (o_orderkey = 231);

-- select distinct on router joins of views also works
SELECT distinct(o_orderkey) FROM priority_orders join air_shipped_lineitems
	ON (o_orderkey = l_orderkey)
	WHERE (o_orderkey = 231);

-- left join support depends on flattening of the query
-- following query fails since the inner part is kept as subquery
SELECT * FROM priority_orders left join air_shipped_lineitems ON (o_orderkey = l_orderkey);

-- however, this works
SELECT count(*) FROM priority_orders left join lineitem_hash_part ON (o_orderkey = l_orderkey) WHERE l_shipmode ='AIR';

-- view at the inner side of is not supported
SELECT count(*) FROM priority_orders right join lineitem_hash_part ON (o_orderkey = l_orderkey) WHERE l_shipmode ='AIR';

-- but view at the outer side is. This is essentially the same as a left join with arguments reversed.
SELECT count(*) FROM lineitem_hash_part right join priority_orders ON (o_orderkey = l_orderkey) WHERE l_shipmode ='AIR';

-- left join on router query is supported
SELECT o_orderkey, l_linenumber FROM priority_orders left join air_shipped_lineitems ON (o_orderkey = l_orderkey)
	WHERE o_orderkey = 2;

-- repartition query on view join
-- it passes planning, fails at execution stage
SELECT * FROM priority_orders JOIN air_shipped_lineitems ON (o_custkey = l_suppkey);

SET citus.task_executor_type to "task-tracker";
SELECT count(*) FROM priority_orders JOIN air_shipped_lineitems ON (o_custkey = l_suppkey);
SET citus.task_executor_type to DEFAULT;

-- materialized views work
-- insert into... select works with views
CREATE TABLE temp_lineitem(LIKE lineitem_hash_part);
SELECT create_distributed_table('temp_lineitem', 'l_orderkey', 'hash', 'lineitem_hash_part');
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems;
SELECT count(*) FROM temp_lineitem;
-- following is a where false query, should not be inserting anything
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems WHERE l_shipmode = 'MAIL';
SELECT count(*) FROM temp_lineitem;

-- can create and query materialized views
CREATE MATERIALIZED VIEW mode_counts
AS SELECT l_shipmode, count(*) FROM temp_lineitem GROUP BY l_shipmode;

SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

-- materialized views are local, cannot join with distributed tables
SELECT count(*) FROM mode_counts JOIN temp_lineitem USING (l_shipmode);

-- new data is not immediately reflected in the view
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems;
SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

-- refresh updates the materialised view with new data
REFRESH MATERIALIZED VIEW mode_counts;
SELECT * FROM mode_counts WHERE l_shipmode = 'AIR' ORDER BY 2 DESC, 1 LIMIT 10;

DROP MATERIALIZED VIEW mode_counts;

SET citus.task_executor_type to "task-tracker";

-- single view repartition subqueries are not supported
SELECT l_suppkey, count(*) FROM
	(SELECT l_suppkey, l_shipdate, count(*)
		FROM air_shipped_lineitems GROUP BY l_suppkey, l_shipdate) supps
	GROUP BY l_suppkey ORDER BY 2 DESC, 1 LIMIT 5;

-- logically same query without a view works fine
SELECT l_suppkey, count(*) FROM
	(SELECT l_suppkey, l_shipdate, count(*)
		FROM lineitem_hash_part WHERE l_shipmode = 'AIR' GROUP BY l_suppkey, l_shipdate) supps
	GROUP BY l_suppkey ORDER BY 2 DESC, 1 LIMIT 5;

-- when a view is replaced by actual query it still fails
SELECT l_suppkey, count(*) FROM
	(SELECT l_suppkey, l_shipdate, count(*)
		FROM (SELECT * FROM lineitem_hash_part WHERE l_shipmode = 'AIR') asi
		GROUP BY l_suppkey, l_shipdate) supps
	GROUP BY l_suppkey ORDER BY 2 DESC, 1 LIMIT 5;

-- repartition query on view with single table subquery
CREATE VIEW supp_count_view AS SELECT * FROM (SELECT l_suppkey, count(*) FROM lineitem_hash_part GROUP BY 1) s1;
SELECT * FROM supp_count_view ORDER BY 2 DESC, 1 LIMIT 10;

SET citus.task_executor_type to DEFAULT;

-- create a view with aggregate
CREATE VIEW lineitems_by_shipping_method AS
	SELECT l_shipmode, count(*) as cnt FROM lineitem_hash_part GROUP BY 1;

-- following will be supported via recursive planning
SELECT * FROM  lineitems_by_shipping_method ORDER BY 1,2 LIMIT 5;

-- create a view with group by on partition column
CREATE VIEW lineitems_by_orderkey AS
	SELECT 
		l_orderkey, count(*) 
	FROM 
		lineitem_hash_part 
	GROUP BY 1;

-- this should work since we're able to push down this query
SELECT * FROM  lineitems_by_orderkey ORDER BY 2 DESC, 1 ASC LIMIT 10;

-- it would also work since it is made router plannable
SELECT * FROM  lineitems_by_orderkey WHERE l_orderkey = 100;

DROP TABLE temp_lineitem CASCADE;

DROP VIEW supp_count_view;
DROP VIEW lineitems_by_orderkey;
DROP VIEW lineitems_by_shipping_method;
DROP VIEW air_shipped_lineitems;
DROP VIEW priority_lineitem;
DROP VIEW priority_orders;

-- new tests for real time use case including views and subqueries

-- create view to display recent user who has an activity after a timestamp
CREATE VIEW recent_users AS
	SELECT user_id, max(time) as lastseen FROM users_table
	GROUP BY user_id
	HAVING max(time) > '2017-11-23 16:20:33.264457'::timestamp order by 2 DESC; 
SELECT * FROM recent_users;

-- create a view for recent_events
CREATE VIEW recent_events AS
	SELECT user_id, time FROM events_table
	WHERE time > '2017-11-23 16:20:33.264457'::timestamp;

SELECT count(*) FROM recent_events;

-- count number of events of recent_users
SELECT count(*) FROM recent_users ru JOIN events_table et ON (ru.user_id = et.user_id);
-- count number of events of per recent users order by count
SELECT ru.user_id, count(*) 
	FROM recent_users ru 
		JOIN events_table et
		ON (ru.user_id = et.user_id)
	GROUP BY ru.user_id
	ORDER BY 2 DESC, 1;

-- the same query with a left join however, it would still generate the same result
SELECT ru.user_id, count(*) 
	FROM recent_users ru 
		LEFT JOIN events_table et
		ON (ru.user_id = et.user_id)
	GROUP BY ru.user_id
	ORDER BY 2 DESC, 1;

-- query wrapped inside a subquery, it needs another top level order by
SELECT * FROM
	(SELECT ru.user_id, count(*) 
		FROM recent_users ru 
			JOIN events_table et
			ON (ru.user_id = et.user_id)
		GROUP BY ru.user_id
		ORDER BY 2 DESC, 1) s1
ORDER BY 2 DESC, 1;

-- non-partition key joins are not supported inside subquery
SELECT * FROM
	(SELECT ru.user_id, count(*) 
		FROM recent_users ru 
			JOIN events_table et
			ON (ru.user_id = et.event_type)
		GROUP BY ru.user_id
		ORDER BY 2 DESC, 1) s1
ORDER BY 2 DESC, 1;

-- join between views
-- recent users who has an event in recent events
SELECT ru.user_id FROM recent_users ru JOIN recent_events re USING(user_id) GROUP BY ru.user_id ORDER BY ru.user_id;

-- outer join inside a subquery
-- recent_events who are not done by recent users
SELECT count(*) FROM (
	SELECT re.*, ru.user_id AS recent_user
		FROM recent_events re LEFT JOIN recent_users ru USING(user_id)) reu 
	WHERE recent_user IS NULL;

-- same query with anti-join
SELECT count(*)
	FROM recent_events re LEFT JOIN recent_users ru ON(ru.user_id = re.user_id)
	WHERE ru.user_id IS NULL;

-- join between view and table
-- users who has recent activity and they have an entry with value_1 is less than 3
SELECT ut.* FROM recent_users ru JOIN users_table ut USING (user_id) WHERE ut.value_1 < 3 ORDER BY 1,2;

-- determine if a recent user has done a given event type or not
SELECT ru.user_id, CASE WHEN et.user_id IS NULL THEN 'NO' ELSE 'YES' END as done_event
	FROM recent_users ru
	LEFT JOIN events_table et
	ON(ru.user_id = et.user_id AND et.event_type = 6)
	ORDER BY 2 DESC, 1;

-- view vs table join wrapped inside a subquery
SELECT * FROM
	(SELECT ru.user_id, CASE WHEN et.user_id IS NULL THEN 'NO' ELSE 'YES' END as done_event
		FROM recent_users ru
		LEFT JOIN events_table et
		ON(ru.user_id = et.user_id AND et.event_type = 6)
	) s1
ORDER BY 2 DESC, 1;

-- event vs table non-partition-key join is not supported
SELECT * FROM
	(SELECT ru.user_id, CASE WHEN et.user_id IS NULL THEN 'NO' ELSE 'YES' END as done_event
		FROM recent_users ru
		LEFT JOIN events_table et
		ON(ru.user_id = et.event_type)
	) s1
ORDER BY 2 DESC, 1;

-- create a select only view
CREATE VIEW selected_users AS SELECT * FROM users_table WHERE value_1 >= 1 and value_1 <3;
CREATE VIEW recent_selected_users AS SELECT su.* FROM selected_users su JOIN recent_users ru USING(user_id);

SELECT user_id FROM recent_selected_users GROUP BY 1 ORDER BY 1;

-- this would be supported when we implement where partition_key in (subquery) support
SELECT et.user_id, et.time FROM events_table et WHERE et.user_id IN (SELECT user_id FROM recent_selected_users) GROUP BY 1,2 ORDER BY 1 DESC,2 DESC LIMIT 5;

-- it is supported when it is a router query
SELECT count(*) FROM events_table et WHERE et.user_id IN (SELECT user_id FROM recent_selected_users WHERE user_id = 1);

-- union between views is supported through recursive planning
(SELECT user_id FROM recent_users) 
UNION
(SELECT user_id FROM selected_users)
ORDER BY 1;

-- wrapping it inside a SELECT * works
SELECT *
	FROM (
		(SELECT user_id FROM recent_users) 
		UNION
		(SELECT user_id FROM selected_users) ) u
	WHERE user_id < 2 AND user_id > 0
	ORDER BY user_id;

-- union all also works for views
SELECT *
	FROM (
		(SELECT user_id FROM recent_users) 
		UNION ALL
		(SELECT user_id FROM selected_users) ) u
	WHERE user_id < 2 AND user_id > 0
	ORDER BY user_id;

SELECT count(*)
	FROM (
		(SELECT user_id FROM recent_users) 
		UNION
		(SELECT user_id FROM selected_users) ) u
	WHERE user_id < 2 AND user_id > 0;

-- UNION ALL between views is supported through recursive planning
SELECT count(*)
	FROM (
		(SELECT user_id FROM recent_users) 
		UNION ALL
		(SELECT user_id FROM selected_users) ) u
	WHERE user_id < 2 AND user_id > 0;

-- expand view definitions and re-run last 2 queries
SELECT count(*)
	FROM (
		(SELECT user_id FROM (SELECT user_id, max(time) as lastseen FROM users_table
			GROUP BY user_id
			HAVING max(time) > '2017-11-22 05:45:49.978738'::timestamp order by 2 DESC) aa
		) 
		UNION
		(SELECT user_id FROM (SELECT * FROM users_table WHERE value_1 >= 1 and value_1 < 3) bb) ) u
	WHERE user_id < 2 AND user_id > 0;

SELECT count(*)
	FROM (
		(SELECT user_id FROM (SELECT user_id, max(time) as lastseen FROM users_table
			GROUP BY user_id
			HAVING max(time) > '2017-11-22 05:45:49.978738'::timestamp order by 2 DESC) aa
		) 
		UNION ALL
		(SELECT user_id FROM (SELECT * FROM users_table WHERE value_1 >= 1 and value_1 < 3) bb) ) u
	WHERE user_id < 2 AND user_id > 0;

-- test distinct
-- distinct is supported if it is on a partition key
CREATE VIEW distinct_user_with_value_1_3 AS SELECT DISTINCT user_id FROM users_table WHERE value_1 = 3;
SELECT * FROM distinct_user_with_value_1_3 ORDER BY user_id;

-- distinct is not supported if it is on a non-partition key
-- but will be supported via recursive planning
CREATE VIEW distinct_value_1 AS SELECT DISTINCT value_1 FROM users_table WHERE value_2 = 3;
SELECT * FROM distinct_value_1 ORDER BY 1 DESC LIMIT 5;

-- CTEs are supported even if they are on views
CREATE VIEW cte_view_1 AS
WITH c1 AS (SELECT * FROM users_table WHERE value_1 = 3) SELECT * FROM c1 WHERE value_2 < 4;

SELECT * FROM cte_view_1 ORDER BY 1,2,3,4,5 LIMIT 5;

-- this is single shard query and still not supported since it has view + cte
-- router planner can't detect it
SELECT * FROM cte_view_1 WHERE user_id = 2 ORDER BY 1,2,3,4,5;

-- if CTE itself prunes down to a single shard than the view is supported (router plannable)
CREATE VIEW cte_view_2 AS
WITH c1 AS (SELECT * FROM users_table WHERE user_id = 2) SELECT * FROM c1 WHERE value_1 = 3;
SELECT * FROM cte_view_2;

CREATE VIEW router_view AS SELECT * FROM users_table WHERE user_id = 2;
-- router plannable
SELECT user_id FROM router_view GROUP BY 1;

-- join a router view
 SELECT * FROM (SELECT user_id FROM router_view GROUP BY 1) rv JOIN recent_events USING (user_id) ORDER BY 2 LIMIT 3;
 SELECT * FROM (SELECT user_id FROM router_view GROUP BY 1) rv JOIN (SELECT * FROM recent_events) re USING (user_id) ORDER BY 2 LIMIT 3;

-- views with limits
CREATE VIEW recent_10_users AS
	SELECT user_id, max(time) as lastseen FROM users_table
	GROUP BY user_id
	ORDER BY lastseen DESC
	LIMIT 10;

-- this is not supported since it has limit in it and subquery_pushdown is not set
SELECT * FROM recent_10_users;

SET citus.subquery_pushdown to ON;
-- still not supported since outer query does not have limit
-- it shows a different (subquery with single relation) error message
SELECT * FROM recent_10_users;
-- now it displays more correct error message
SELECT et.* FROM recent_10_users JOIN events_table et USING(user_id);

-- now both are supported when there is a limit on the outer most query
SELECT * FROM recent_10_users ORDER BY lastseen DESC LIMIT 10;
SELECT et.* FROM recent_10_users JOIN events_table et USING(user_id) ORDER BY et.time DESC LIMIT 10;

RESET citus.subquery_pushdown;

VACUUM ANALYZE users_table;

-- explain tests
EXPLAIN (COSTS FALSE) SELECT user_id FROM recent_selected_users GROUP BY 1 ORDER BY 1;

EXPLAIN (COSTS FALSE) SELECT *
	FROM (
		(SELECT user_id FROM recent_users) 
		UNION
		(SELECT user_id FROM selected_users) ) u
	WHERE user_id < 4 AND user_id > 1
	ORDER BY user_id;

EXPLAIN (COSTS FALSE) SELECT et.* FROM recent_10_users JOIN events_table et USING(user_id) ORDER BY et.time DESC LIMIT 10;
SET citus.subquery_pushdown to ON;
EXPLAIN (COSTS FALSE) SELECT et.* FROM recent_10_users JOIN events_table et USING(user_id) ORDER BY et.time DESC LIMIT 10;

RESET citus.subquery_pushdown;

DROP VIEW recent_10_users;
DROP VIEW router_view;
DROP VIEW cte_view_2;
DROP VIEW cte_view_1;
DROP VIEW distinct_value_1;
DROP VIEW distinct_user_with_value_1_3;
DROP VIEW recent_selected_users;
DROP VIEW selected_users;
DROP VIEW recent_events;
DROP VIEW recent_users;
