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

-- count distinct on partition column is not supported
SELECT count(distinct o_orderkey) FROM priority_orders join air_shipped_lineitems ON (o_orderkey = l_orderkey);

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

-- insert into... select works with views
CREATE TABLE temp_lineitem(LIKE lineitem_hash_part);
SELECT create_distributed_table('temp_lineitem', 'l_orderkey', 'hash', 'lineitem_hash_part');
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems;
SELECT count(*) FROM temp_lineitem;
-- following is a where false query, should not be inserting anything
INSERT INTO temp_lineitem SELECT * FROM air_shipped_lineitems WHERE l_shipmode = 'MAIL';
SELECT count(*) FROM temp_lineitem;

-- modifying views is disallowed
INSERT INTO air_shipped_lineitems SELECT * from temp_lineitem;

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

SET citus.task_executor_type to DEFAULT;

-- create a view with aggregate
CREATE VIEW lineitems_by_shipping_method AS
	SELECT l_shipmode, count(*) as cnt FROM lineitem_hash_part GROUP BY 1;

-- following will fail due to non-flattening of subquery due to GROUP BY
SELECT * FROM  lineitems_by_shipping_method;

-- create a view with group by on partition column
CREATE VIEW lineitems_by_orderkey AS
	SELECT l_orderkey, count(*) FROM lineitem_hash_part GROUP BY 1;

-- this will also fail due to same reason
SELECT * FROM  lineitems_by_orderkey;

-- however it would work if it is made router plannable
SELECT * FROM  lineitems_by_orderkey WHERE l_orderkey = 100;

DROP TABLE temp_lineitem CASCADE;
