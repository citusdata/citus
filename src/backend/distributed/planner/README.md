# Distributed Query Planner

The distributed query planner is entered through the `distributed_planner` function in `distributed_planner.c`. This is the hook that Postgres calls instead of `standard_planner`.

If the input query is trivial (e.g., no joins, no subqueries/ctes, single table and single shard), we create a very simple `PlannedStmt`. If the query is not trivial, call `standard_planner` to build a `PlannedStmt`. For queries containing a distributed table or reference table, we then proceed with distributed planning, which overwrites the `planTree` in the `PlannedStmt`.

Distributed planning (`CreateDistributedPlan`) tries several different methods to plan the query:


 1. Fast-path router planner, proceed if the query prunes down to a single shard of a single table
 2. Router planner, proceed if the query prunes down to a single set of co-located shards
 3. Modification planning, proceed if the query is a DML command and all joins are co-located
 4. Recursive planning, find CTEs and subqueries that cannot be pushed down and go back to 1
 5. Logical planner, constructs a multi-relational algebra tree to find a distributed execution plan

## Fast-path router planner

By examining the query tree, if we can decide that the query hits only a single shard of a single table, we can skip calling `standard_planner()`. Later on the execution, we simply fetch the filter on the distribution key and do the pruning.

As the name reveals, this can be considered as a sub-item of Router planner described below. The only difference is that fast-path planner doesn't rely on `standard_planner()` for collecting restriction information.


## Router planner

During the call to `standard_planner`, Postgres calls a hook named `multi_relation_restriction_hook`. We use this hook to determine explicit and implicit filters on (occurrences of) distributed tables. We apply shard pruning to all tables using the filters in `PlanRouterQuery`. If all tables prune down to a single shard and all those shards are on the same node, then the query is router plannable meaning it can be fully executed by one of the worker nodes.

The router planner preserves the original query tree, but the query does need to be rewritten to have shard names instead of table names. We cannot simply put the shard names into the query tree, because it actually contains relation OIDs. If the deparsing logic resolves those OIDs, it would throw an error since the shards do not exist on the coordinator. Instead, we replace the table entries with a fake SQL function call to `citus_extradata_container` and encode the original table ID and the shard ID into the parameter of the function. The deparsing functions recognise the fake function call and convert it into a shard name.

## Recursive planning

CTEs and subqueries that cannot be pushed down (checked using `DeferErrorIfCannotPushdownSubquery`) and do not contain references to the outer query are planned by recursively calling the `planner` function with the subquery as the parse tree. Because the planner is called from the top, any type of query that is supported by Postgres or Citus can be a valid subquery. The resulting plan is added to the `subPlanList` in the `DistributedPlan` and the subquery is replaced by a call to `read_intermediate_result` with a particular subplan name. At execution time, all of the plans in the `subPlanList` are executed and their output is sent to all the workers and written into an intermediate result with the subplan name (see `subplan_execution.c`). In the remainder of the planner, calls to `read_intermediate_result` are treated in the same way as reference tables, which means they can be joined by any column.

## Logical planner

The logical planner constructs a multi-relational algebra tree from the query with operators such as `MultiTable`, `MultiProject`, `MultiJoin` and `MultiCollect`. It first picks a strategy for handling joins in `MultiLogicalPlanCreate` (pushdown planning, or join order planning) and then builds a `MultiNode` tree based on the original query tree. In the initial `MultiNode` tree, each `MultiTable` is wrapped in `MultiCollect`, which effectively means collect the entire table in one place. The `MultiNode` tree is passed to the logical optimizer which transforms the tree into one that requires less network traffic by pushing down operators. Finally, the physical planner transforms the `MultiNode` tree into a `DistributedPlan` which contains the queries to execute on shards and can be passed to the executor.

###  Pushdown planning

During the call to `standard_planner`, Postgres calls a hook named `multi_relation_restriction_hook`. We use this hook to determine whether all (occurrences of) distributed tables are joined on their respective distribution columns. When this is the case, we can be somewhat agnostic to the structure of subqueries and other joins. In that case, we treat the whole join tree as a single `MultiTable` and deparse this part of the query as is during physical planning. Pushing down a subquery is only possible when the subquery can be answered without a merge step (checked using `DeferErrorIfCannotPushdownSubquery`). However, you may notice that these subqueries are already replaced by `read_intermediate_result` calls during recursive planning. Only subqueries that have references to the outer query remain at this stage would pass through recursive planning and fail the check.

### Join order planning

The join order planner is applied to the join tree in the original query and generates all possible pair-wise join orders. If a pair-wise join is not on the distribution column, it requires re-partitioning. The join order that requires the fewest re-partition steps is converted into a tree of `MultiJoin` nodes, prior to running the logical optimizer. The optimizer leaves the `MultiJoin` tree in tact, but pushes down select and collect operators below the `MultiJoin` nodes.

### Logical optimizer

The logical optimizer uses commutativity rules to push project and select operators down below the `MultiCollect` nodes. Everything above the `MultiCollect` operator will be is executed on the coordinator and everything below on the workers. Additionally, the optimizer uses distributivity rules to push down operators below the `MultiJoin` nodes, such that filters and projections are applied prior to joins. This is primarily relevant for re-partition joins which first try to reduce the data by applying selections and projections, and then re-partitioning the result.

A number of SQL clauses like aggregates, GROUP BY, ORDER BY, LIMIT can only be pushed down below the `MultiCollect` under certain conditions. All these clauses are bundled together in a `MultiExtendedOpNode`. After the basic transformation, the `MultiExtendedOpNode`s are directly above the `MultiCollect` nodes. They are then split into a coordinator and a worker part and the worker part is pushed down below the `MultiCollect`.

### Physical planner

This section needs to be expanded.

## Modification planning

In terms of modification planning, we distinguish between several cases:

 1. DML planning (`CreateModifyPlan`)
 1.a. UPDATE/DELETE planning
 1.b. INSERT planning
 2. INSERT...SELECT planning (`CreateInsertSelectPlan`)

### UPDATE/DELETE planning

UPDATE and DELETE commands are handled by the router planner (`PlanRouterQuery`), but when tables prune to multiple shards we do not fall back to other planners, but instead proceed to generate a task for each shard, as long as all subqueries can be pushed down. We can do this because UPDATE and DELETE never have a meaningful merge step on the coordinator, other than concatening RETURNING rows.

When there are CTEs or subqueries that cannot be pushed down in the UPDATE/DELETE, we continue with recursive planning and try again. If recursive planning cannot resolve the issue (e.g. due to a correlated subquery), then the command will error out.

### INSERT planning

Distributed planning for INSERT commands is relatively complicated because of multi-row INSERT. Each row in a multi-row INSERT may go to different shard and therefore we need to construct a different query for each shard. The logic for this is primarily in `BuildRoutesForInsert`, which builds the set of rows for each shard. Rather than construct a full query tree for each shard, we put each set of rows in the `rowValuesLists` of the `Task` and replace the VALUES section of the INSERT just before deparsing in `UpdateTaskQueryString`.

One additional complication for INSERTs is that it is very common to have a function call (such as `now()` or `nextval(..)`) in the position of the distribution column. In that case building the task list is deferred until the functions have been evaluated in the executor.

### INSERT ... SELECT query planning

Citus supports `INSERT ... SELECT` queries either by pushing down the whole query to the worker nodes or pulling the `SELECT` part to the coordinator.

#### INSERT ... SELECT - by pushing down

If `INSERT ... SELECT` query can be planned by pushing down it to the worker nodes, Citus selects to choose that logic first. Query is planned separately for each shard in the target table. Do so by replacing the partitioning qual parameter using the shard's actual boundary values to create modify task for each shard. Then, shard pruning is performed to decide on to which shards query will be pushed down. Finally, checks if the target shardInterval has exactly same placements with the select task's available anchor placements.

#### INSERT...SELECT - via the coordinator

If the query can not be pushed down to the worker nodes, two different approaches can be followed depending on whether ON CONFLICT or RETURNING clauses are used.

* If `ON CONFLICT` or `RETURNING` are not used, Citus uses `COPY` command to handle such queries. After planning the `SELECT` part of the `INSERT ... SELECT` query, including subqueries and CTEs, it executes the plan and send results back to the DestReceiver which is created using the target table info.

* Since `COPY` command supports neither `ON CONFLICT` nor `RETURNING` clauses, Citus perform `INSERT ... SELECT` queries with `ON CONFLICT` or `RETURNING` clause in two phases. First, Citus plans the `SELECT` part of the query, executes the plan and saves results to the intermediate table which is colocated with target table of the `INSERT ... SELECT` query. Then, `INSERT ... SELECT` query is directly run on the worker node using the intermediate table as the source table.
