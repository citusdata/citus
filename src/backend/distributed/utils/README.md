# Cluster Clock
### Requirement:
Many distributed applications need to track the changes in the same order as they are applied on the database. The changes can be to databases or objects within them, either within a single node or across the sharded cluster.
### Definitions
**Total ordering** - Every pair of change events can be placed in some order.

**Causal ordering** - Only events that are causally related (an event A caused an event B) can be ordered i.e., it's only a partial order - sometimes events happen independently --with no possible causal relationship--, such events are treated as concurrent.

**Sequential consistency** - All writes must be seen in the same order by all processes.

**Causal consistency** - Causally related writes must be seen in the same order.

Transactions on a single node system naturally provide a total and sequential ordering guarantees for client read and write operations as all operations are routed to the same node, but there are challenges for a multi node distributed system, such as, Citus.

One possible way to totally order all the changes in the system is to timestamp all the events with a global physical clock or a centralized logical clock. Thus, observing the events in the increasing order of the timestamp will give the total ordering of events. For both the performance and cost reasons such solutions are impractical. In the absence of total ordering, a little weaker ordering is the **causal order**

Causal order is defined as a model that preserves a partial order of events in a distributed system. If an event

1.  A causes another event B, every other process in the system observes the event A before observing event B.
2.  Causal order is transitive: if A causes B, and B causes C, then A causes C.
3.  Non causally ordered events are treated as concurrent.

Causal consistency is a weak form of consistency that preserves the order of causally related operations. The causal consistency model can be refined into four session guarantees.

1.  Read Your Writes: If a process performs a write, the same process later observes the result of its write.
6.  Monotonic Reads: The set of writes observed (read) by a process is guaranteed to be monotonically increasing.
7.  Writes Follow Reads: If some process performs a read followed by a write, and another process observes the result of the write, then it can also observe the read.
8.  Monotonic Writes: If some process performs a write, followed sometime later by another write, other processes will observe them in the same order.

### Hybrid Logical Clock (HLC)
HLC provides a way to get the causality relationship like logical clocks. It can also be used for backup/recovery too as the logical clock value is maintained close to the wall clock time. HLC consists of
 LC - Logical clock
 C - Counter

Clock components - Unsigned 64 bit <LC, C>

Epoch Milliseconds ( LC ) | Logical counter ( C )|
|--|--|
| 42 bits | 22 bits |


2^42 milliseconds - 4398046511104 milliseconds, which is ~139 years.

2^22 ticks - maximum of four million operations per millisecond.

### New catalog
All the committed transactions are persisted with the transaction id and the commit clock time in a new catalog

**pg_dist_commit_transaction**

    Transaction Identifier
     (database_id, process_id, initiator_node_identifier, transaction_number, transaction_stamp)

Assuming timestamp never jumps back, this id is globally unique across the cluster and restarts.

|TransactionId| CommitClock (LC, C)| timestamp (epoch) |
|--|--|--|
| (13090,1077913,2,5,"2022-07-26 19:05:09.290597-07") |(1658887880235, 9) | 2022-07-26 19:05:09.290597-07

### GUC
A new GUC parameter, "**citus.enable_global_clock**", enables cluster-wide timestamp for all the transactions and persists them in the table.

### Psuedo code
    WC - Current Wall Clock in milliseconds
    HLC - Current Hybrid Logical Clock in shared memory
    MAXTICKS - Four million

    /* Tick for each event */
    GetClock()
    {
	    IF (HLC.LC < WC)
		    HLC.LC = WC;
		    HLC.C = 0;
	    ELSE IF (HLC.C == MAXTICKS)
		    HLC.LC = HLC.LC + 1;
		    HLC.C = 0;
	    ELSE
		    HLC.C = HLC.C + 1;
		    return HLC;
	}

	/* Returns true if the clock1 is after clock2*/
    IsEventAfter(HLC1, HLC2)
    {
	    IF (HLC1.LC != HLC2.LC)
		    return (HLC1.LC > HLC2.LC);
		ELSE
			return (HLC1.C > HLC2.C);
    }

    /* Simply returns the highest cluster clock value */
    CurrentClusterClock()
    {
	    For each node
	    {
		    NodeClock[N] = GetClock();
		}

		/* Return the highest clock value of all the nodes */
		return max(NodeClock[N]);
    }

    /* Adjust the local shared memory clock to the
	received value from the remote node */
	ReceiveClock(RHLC)
	{
		IF (RHLC.LC < HLC.LC)
			return; /* do nothing */

		IF (RHLC.LC > HLC.LC)
			HLC.LC = RHLC.LC;
			HLC.C = RHLC.C;
			return;

		IF (RHLC.LC == HLC.LC)
			HLC.C = (RHLC.C > HLC.C) ? RHLC.C : HLC.C;
	}

    /* All the nodes will adjust their clocks to the
    highest of the newly committed 2PC */
    AdjustClocks(HLC)
    {
	    For each node
	    {
		    SendCommand("select ReceiveClock(%s)", HLC);
		}
    }

    /* During prepare, once all the nodes acknowledge
    commit, persist the current transaction id along with
    the clock value in the catalog */
    PrepareTransaction()
    {
		HLC = CurrentClusterClock();
		PersistCatalog(get_current_transaction_id(), HLC);
		AdjustClocks(HLC);
    }

    /* Initialize the shared memory clock value to the
    highest clock persisted */
    InitClockAtBoot()
    {
	    HLC.LC = WC;
	    HLC.C = 0;
	    MAX_LC = "SELECT max(CommitClock.LC) FROM catalog";

	    IF (MAX_LC != NULL)
	    {
		    /*There are prior commits, adjust to that value*/
		    ReceiveClock(MAX_LC);
	    }
    }

 #### Usage
**Step 1**
In the application, track individual changes with the current transaction id

    UPDATE track_table
    SET TransactionId = get_current_transaction_id(), operation = <>, row_key = <>,....;

**Step 2**
As the transaction commits, if the GUC is enabled, engine internally calls `citus_get_cluster_clock()` and persists the current transaction Id along with the commit cluster clock.

    INSERT INTO pg_dist_commit_transaction(TransactionId, CommitClock, timestamp) VALUES (current_transactionId, commit_clock, now())

**Step 3**
How to get all the events in the causal order?

    SELECT tt.row_key, tt.operation
    FROM track_table tt, pg_dist_commit_transaction cc
    WHERE tt.TransactionId = cc.TransactionId
    ORDER BY cc.CommitClock

Events for an object

    SELECT tt.row_key, tt.operation
    FROM track_table tt, pg_dist_commit_transaction cc
    WHERE tt.TransactionId = cc.TransactionId
    and row_key = $1 ORDER BY cc.CommitClock

Events in the last one hour

    SELECT tt.row_key, tt.operation
    FROM track_table tt, pg_dist_commit_transaction cc
    WHERE cc.timestamp >= now() - interval '1 hour'
    and tt.TransactionId = cc.TransactionId

Note: In Citus we use 2PC, if any node goes down after the PREPARE and before COMMIT, we might have changes partially committed. Citus tracks such transactions in **pg_dist_transaction** and eventually be committed when the node becomes healthy, when we track data from committed transaction of **pg_dist_commit_transaction** we will miss the changes from the bad-node.

One way to avoid such anomaly, take only the transactions from **pg_dist_commit_transaction** with clock value less than the minimum clock of the transactions in **pg_dist_transaction**. Caveat is, if the node takes long to recover and the 2PC to fully recover, the visibility of the committed transactions might stall.

### Catalog pruning
The data in **pg_dist_commit_transaction** should be ephemeral data i.e., eventually rows have to automatically be deleted. Users can install a pg_cron job to prune the catalog regularly.

    delete from pg_dist_commit_transaction
        where timestamp < now() - interval '7 days'

### Limitations of Citus
Using this transaction commit clock ordering to build a secondary, that's a mirror copy of the original, may not be feasible at this time for the following reasons.

Given that there is no well-defined order between concurrent distributed transactions in Citus, we cannot retroactively apply a transaction-order that leads to an exact replica of the primary unless we preserve the original object-level ordering as it happened on individual nodes.

For instance, if a multi-shard insert (transaction A) happens concurrently with a multi-shard update (transaction B) and the WHERE clause of the update matches inserted rows in multiple shards, we could have a scenario in which only a subset of the inserted rows gets updated. Effectively, transaction A might happen before transaction B on node 1, while transaction B happens before transaction A on node 2. While unfortunate, we cannot simply claim changes made by transaction A happened first based on commit timestamps, because that would lead us reorder changes to the same object ID on node 2, which might lead to a different outcome when replayed.

In such scenario, even if we use causal commit clock to order changes. It is essential that the order of modifications to an object matches the original order. Otherwise, you could have above scenarios where an insert happens before an update in the primary cluster, but the update happens before the insert. Replaying the changes would then lead to a different database.

In absence of a coherent transaction-ordering semantics in distributed cluster, best we can do is ensure that changes to the same object are in the correct order and ensure exactly once delivery (correct pagination).

