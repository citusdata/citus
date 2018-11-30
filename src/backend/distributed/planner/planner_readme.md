# INSERT ... SELECT query planning

Citus supports `INSERT ... SELECT` queries either by pushing down the whole query to the worker nodes or pulling the `SELECT` part to the coordinator.

## INSERT ... SELECT - by pushing down

If `INSERT ... SELECT` query can be planned by pushing down it to the worker nodes, Citus selects to choose that logic first. Query is planned separately for each shard in the target table. Do so by replacing the partitioning qual parameter using the shard's actual boundary values to create modify task for each shard. Then, shard pruning is performed to decide on to which shards query will be pushed down. Finally, checks if the target shardInterval has exactly same placements with the select task's available anchor placements.

## INSERT...SELECT - via the coordinator

If the query can not be pushed down to the worker nodes, two different approaches can be followed depending on whether ON CONFLICT or RETURNING clauses are used.

* If `ON CONFLICT` or `RETURNING` are not used, Citus uses `COPY` command to handle such queries. After planning the `SELECT` part of the `INSERT ... SELECT` query, including subqueries and CTEs, it executes the plan and send results back to the DestReceiver which is created using the target table info.

* Since `COPY` command supports neither `ON CONFLICT` nor `RETURNING` clauses, Citus perform `INSERT ... SELECT` queries with `ON CONFLICT` or `RETURNING` clause in two phases. First, Citus plans the `SELECT` part of the query, executes the plan and saves results to the intermediate table which is colocated with target table of the `INSERT ... SELECT` query. Then, `INSERT ... SELECT` query is directly run on the worker node using the intermediate table as the source table.
