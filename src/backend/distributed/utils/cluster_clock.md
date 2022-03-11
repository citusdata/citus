# Requirement
Many distributed applications need to track the changes in the same order as they are applied on the database. The changes can be to databases or objects within them, either in a single node or across the sharded cluster. <br>
**Note**: Not to be confused with serialization or any other external imposed order, the changes happen the usual way in the system.

## Definitions
**Total ordering** - Every pair of change events can be placed in some order. <br>
**Causal ordering** - Only events that are causally related (an event A caused an event B) can be ordered i.e., it's only a partial order - sometimes events happen independently with no possible causal relationship, such events are treated to concurrent. <br>
**Sequential consistency** - All writes must be seen in the same order by all processes. <br>
**Causal consistency** - Causally related writes must be seen in the same order. <br>

## Introduction
Transactions in a single node system naturally provide a total and sequential ordering guarantees for client read and write operations as all operations are routed to the same node, but there are challenges for a multi node distributed system, such as, Citus.

One possible way to totally order all the changes in the system is to timestamp all the events with a global physical clock or a centralized logical clock. Thus, observing the events in the increasing order of the timestamp will give the total ordering of events. For both performance and cost reasons such solutions are impractical. In the absence of total ordering, a little weaker ordering is the **causal order**

Causal order is defined as a model that preserves a partial order of events in a distributed system. If an event
 1. A causes another event B, every other process in the system observes the event A before observing event B.
 2. Causal order is transitive: if A causes B, and B causes C, then A causes C.
 3. Non causally ordered events are treated as concurrent.

Causal consistency is a weak form of consistency that preserves the order of causally related operations. The causal consistency model can be refined into four session guarantees
 1. Read Your Writes: If a process performs a write, the same process
    later observes the result of its write.
 2. Monotonic Reads: The set of writes observed (read) by a process is
    guaranteed to be monotonically increasing.
 3. Writes Follow Reads: If some process performs a read followed by a write, and another process observes the result of the write, then it can also observe the read.
 4. Monotonic Writes: If some process performs a write, followed sometime later by another write, other processes will observe them in the same order.

## UDF
***get_cluster_clock()*** is a new UDF that **helps** applications/clients in causally ordering events in the distributed system.

 1. An application can call get_cluster_clock(), which returns monotonically increasing logical clock value, as close to epoch value (in milli seconds) as possible, with the guarantee that it will never go back from its current value even after a restart (not hard crash). The returned value can be used for ordering the events, which are related.

 2. A new GUC value citus.enable_global_clock, when enabled, stamps the cluster wide logical clock value for each distributed transaction. This clock timestamp can be used by applications for causal ordering of changes to the objects in the transaction(s).

    Sample data of cluster wide clock timestamp for a distributed transaction that accesses shards from 3 nodes. The query output is run right after the commit of the transaction.

    SELECT  initiator_node_identifier, transactionclock as commit_clock, global_pid, transaction_stamp
from **get_all_active_transactions()**
where global_pid = citus_backend_gpid()
order by global_pid;


		initiator_node_identifier | commit_clock  | global_pid  |   transaction_stamp
		---------------------------+---------------+-------------+------------------------
                                1 | 1656390052898 | 10000175398 | 1999-12-31 16:00:00-08

	As you can see the transaction has the timestamp of **1656390052898** at the originator and share the same timestamp at all the participating nodes (below output). All the nodes adjust the clock value to the new value, so as to maintain the increasing order of the clock across the cluster.

    SELECT  initiator_node_identifier, transactionclock as commit_clock, global_pid, transaction_stamp
from **get_global_active_transactions()**
where global_pid = citus_backend_gpid()
order by global_pid;

		initiator_node_identifier | commit_clock  | global_pid  |   transaction_stamp
		---------------------------+---------------+-------------+------------------------
                         1 | 1656390052898 | 10000175398 | 1999-12-31 16:00:00-08
                         1 | 1656390052898 | 10000175398 | 1999-12-31 16:00:00-08
                         1 | 1656390052898 | 10000175398 | 1999-12-31 16:00:00-08

### Next steps
 1. The current model doesn't provide protection against clock drifts in the system. On some Posix systems, clock can go backwards after restart of the VM, in which case the increasing order is not guaranteed. This can be alleviated by initializing the clock using new udf ***adjust_cluster_clock***(..) to a user-specified value. Before starting any workload, clock must be set to the most recent clock timestamp seen by the application before restart. This is not an issue for a planned server restart as the shutdown callback persists the most recent value, and on restart, the server adjusts the cluster clock even if there is a drift in the epoch clock value.
 2. Provide a way to get the cluster clock timestamp for any distributed transactions. Currently distributed transactions are uniquely identified by a combination of <br>

	    databaseId
        initiatorNodeIdentifier
        transactionNumber
        timestamp

	Persist cluster clock timestamp with the corresponding unique distributed transaction id in a catalog, which can be either queried directly or a new UDF ***get_transaction_clock***(....) that returns the cluster clock timestamp for the unique distributed transaction id passed as input.
