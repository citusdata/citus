# Cluster Clock
### Requirement:
Many distributed applications need to track the changes in the same order as they are applied on the database. The changes can be to databases or objects within them, either within a single node or across the sharded cluster.
### Definitions
**Total ordering** - Every pair of change events can be placed in some order.
**Causal ordering** - Only events that are causally related (an event A caused an event B) can be ordered i.e., it's only a partial order - sometimes events happen independently with no possible causal relationship, such events are treated to concurrent.
**Sequential consistency** - All writes must be seen in the same order by all processes.
**Causal consistency** - Causally related writes must be seen in the same order.

Transactions on a single node system naturally provide a total and sequential ordering guarantees for client read and write operations as all operations are routed to the same node, but there are challenges for a multi node distributed system, such as, Citus.

One possible way to totally order all the changes in the system is to timestamp all the events with a global physical clock or a centralized logical clock. Thus, observing the events in the increasing order of the timestamp will give the total ordering of events. For both the performance and cost reasons such solutions are impractical. In the absence of total ordering, a little weaker ordering is the **causal order**.

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

### UDFs
A new UDF `citus_get_cluster_clock`() that returns a monotonically increasing logical clock. Clock guarantees to never go back in value after restarts and makes best attempt to keep the value close to UNIX epoch time in milliseconds.

A new UDF `citus_get_transaction_clock`(), when called by the user, returns the logical causal clock timestamp current transaction,
Internally, this is the maximum clock among all transaction nodes, and
all nodes move to the new clock.

### GUC
A new GUC parameter, "**citus.enable_cluster_clock**", If clocks go bad for any reason, this serves as a safety valve to avoid the need to change the application and (re)deploy it.

### Sequence
In Unix, though rare, there is a possibility of clock drifting backwards (or
forward) after a restart. In such rare scenarios, we might end up with a logical clock value less than the previously used value, this violates the fundamental requirement of monotonically increasing clock. To avoid such disasters, every logical clock tick is persisted using sequences (non-transactional). After a restart, the persisted sequence value is read and clock starts from that value, which will ensure that system starts the clock from where we left off.

### Psuedo code
    WC - Current Wall Clock in milliseconds
    HLC - Current Hybrid Logical Clock in shared
          memory
    MAX_COUNTER - Four million

    /* Tick the clock by 1 */
    IncrementClusterClock()
    {
       /* It's the counter that always ticks, once it reaches
          the maximum, reset the counter to 1 and increment
          the logical clock. */

        if (HLC.C == MAX_COUNTER)
        {
                HLC.LC++;
                HLC.C = 0;
                return;
        }
        HLC.C++;
    }

    /* Tick for each event, must increase monotonically */
    GetNextNodeClockValue()
    {
	    IncrementClusterClock(HLC);

	    /* From the incremented clock and current wall clock,
	       pick which ever is highest */
	    NextClock = MAX(HLC, WC);

	    /* Save the NextClock value in both the shared memory
	       and sequence */
	    HLC = NextClock;
	    SETVAL(pg_dist_clock_logical_seq, HLC);
	}

	/* Returns true if the clock1 is after clock2 */
    IsEventAfter(HLC1, HLC2)
    {
	    IF (HLC1.LC != HLC2.LC)
		    return (HLC1.LC > HLC2.LC);
		ELSE
			return (HLC1.C > HLC2.C);
    }

    /* Simply returns the highest node clock value among all
       nodes */
    GetHighestClockInTransaction()
    {
	    For each node
	    {
		    NodeClock[N] = GetNextNodeClockValue();
		}

		/* Return the highest clock value of all the nodes */
		return MAX(NodeClock[N]);
    }

    /* Adjust the local shared memory clock to the received
       value (RHLC) from the remote node */
	AdjustClock(RHLC)
	{
		/* local clock is ahead or equal, do nothing */
        IF (HLC >= RHLC)
        {
          return;
        }
        /* Save the remote clockvalue in both the shared
           memory and sequence */
	    HLC = RHLC;
	    SETVAL(pg_dist_clock_logical_seq, HLC);
	}

    /* All the nodes will adjust their clocks to the highest
       of the newly negotiated clock */
    AdjustClocksToTransactionHighest(HLC)
    {
	    For each node
	    {
		    SendCommand ("AdjustClock(HLC)");
		}
    }

    /* When citus_get_transaction_clock() UDF is invoked */
    PrepareAndSetTransactionClock()
    {
	    /* Pick the highest logical clock value among all
	       transaction-nodes */
        txnCLock = GetHighestClockInTransaction()

        /* Adjust all the nodes with the new clock value */
        AdjustClocksToTransactionHighest(txnCLock )

        return txnClock;
    }

    /* Initialize the clock value to the highest clock
       persisted in sequence */
    InitClockAtBoot()
    {
	    /* Start with the current wall clock */
	    HLC = WC;

	    IF (SEQUENCE == 1)
		/* clock never ticked on this node, start with the
		   wall clock. */
				return;
		/* get the most recent clock ever used from disk */
		persistedClock =
		    NEXT_VAL(pg_dist_clock_logical_seq...)
		    /* Start the clock with persisted value */
		    AdjustLocalClock(persistedClock);
	    }
    }

 #### Usage
**Step 1**
In the application, track every change of a transaction along with the unique transaction ID by calling UDF
`get_current_transaction_id`()

    INSERT INTO track_table
    SET TransactionId =
    get_current_transaction_id(),
    operation = <insert/update/delete>,
    row_key = <>,
    ....;

**Step 2**
As the transaction is about to end, and before the COMMIT, capture the causal clock timestamp along with the transaction ID in a table

    INSERT INTO transaction_commit_clock
    (TransactionId, CommitClock, timestamp)
    SELECT
            citus_get_transaction_clock(),
            get_current_transaction_id(),
            now()

**Step 3**
How to get all the events in the causal order?

    SELECT tt.row_key, tt.operation
    FROM track_table tt,
         transaction_commit_clock cc
    WHERE tt.TransactionId = cc.TransactionId
    ORDER BY cc.CommitClock

Events for an object

    SELECT tt.row_key, tt.operation
    FROM track_table tt,
         transaction_commit_clock cc
    WHERE tt.TransactionId = cc.TransactionId
    and row_key = $1 ORDER BY cc.CommitClock

Events in the last one hour

    SELECT tt.row_key, tt.operation
    FROM track_table tt,
         transaction_commit_clockcc
    WHERE cc.timestamp >= now() - interval '1 hour'
    and tt.TransactionId = cc.TransactionId

**Note**: In Citus we use 2PC, if any node goes down after the PREPARE and before the COMMIT, we might have changes partially committed. Citus tracks such transactions in **pg_dist_transaction** and eventually will be committed when the node becomes healthy, but when we track change-data from committed transactions of **transaction_commit_clock** we will miss the changes from a bad node.

To address this issue, proposal is to have a new UDF #TBD, that freezes
the clock and ensures that all the 2PCs are fully complete
(i.e., **pg_dist_transaction** should be empty) and return the highest
clock used. All transactions in `transaction_commit_clock` with
timestamp below this returned clock are visible to the application. The
exact nuances, such as frequency of calling such UDF, are still TBD.
Caveat is, if the node and the 2PC takes long to fully recover, the
visibility of the committed transactions might stall.

### Catalog pruning
The data in **transaction_commit_clock** should be ephemeral data i.e., eventually rows have to automatically be deleted. Users can install a pg_cron job to prune the catalog regularly.

    delete from transaction_commit_clock
        where timestamp < now() - interval '7 days'

### Limitations of Citus
Using this transaction commit clock ordering to build a secondary, that's a mirror copy of the original, may not be feasible at this time for the following reasons.

Given that there is no well-defined order between concurrent distributed transactions in Citus, we cannot retroactively apply a transaction-order that leads to an exact replica of the primary unless we preserve the original object-level ordering as it happened on individual nodes.

For instance, if a multi-shard insert (transaction A) happens concurrently with a multi-shard update (transaction B) and the WHERE clause of the update matches inserted rows in multiple shards, we could have a scenario in which only a subset of the inserted rows gets updated. Effectively, transaction A might happen before transaction B on node 1, while transaction B happens before transaction A on node 2. While unfortunate, we cannot simply claim changes made by transaction A happened first based on commit timestamps, because that would lead us reorder changes to the same object ID on node 2, which might lead to a different outcome when replayed.

In such scenario, even if we use causal commit clock to order changes. It is essential that the order of modifications to an object matches the original order. Otherwise, you could have above scenarios where an insert happens before an update in the primary cluster, but the update happens before the insert. Replaying the changes would then lead to a different database.

In absence of a coherent transaction-ordering semantics in distributed cluster, best we can do is ensure that changes to the same object are in the correct order and ensure exactly once delivery (correct pagination).
