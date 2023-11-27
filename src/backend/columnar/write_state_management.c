
#include <math.h>

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_trigger.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/plancat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "citus_version.h"
#include "pg_version_compat.h"

#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_version_compat.h"


/*
 * Mapping from relfilenode to WriteStateMapEntry. This keeps write state for
 * each relation.
 */
static HTAB *WriteStateMap = NULL;

/* memory context for allocating WriteStateMap & all write states */
static MemoryContext WriteStateContext = NULL;

/*
 * Each member of the writeStateStack in WriteStateMapEntry. This means that
 * we did some inserts in the subtransaction subXid, and the state of those
 * inserts is stored at writeState. Those writes can be flushed or unflushed.
 */
typedef struct SubXidWriteState
{
	SubTransactionId subXid;
	ColumnarWriteState *writeState;

	struct SubXidWriteState *next;
} SubXidWriteState;


/*
 * An entry in WriteStateMap.
 */
typedef struct WriteStateMapEntry
{
	/* key of the entry */
	RelFileNumber relfilenumber;

	/*
	 * If a table is dropped, we set dropped to true and set dropSubXid to the
	 * id of the subtransaction in which the drop happened.
	 */
	bool dropped;
	SubTransactionId dropSubXid;

	/*
	 * Stack of SubXidWriteState where first element is top of the stack. When
	 * inserts happen, we look at top of the stack. If top of stack belongs to
	 * current subtransaction, we forward writes to its writeState. Otherwise,
	 * we create a new stack entry for current subtransaction and push it to
	 * the stack, and forward writes to that.
	 */
	SubXidWriteState *writeStateStack;
} WriteStateMapEntry;


/*
 * Memory context reset callback so we reset WriteStateMap to NULL at the end
 * of transaction. WriteStateMap is allocated in & WriteStateMap, so its
 * leaked reference can cause memory issues.
 */
static MemoryContextCallback cleanupCallback;
static void
CleanupWriteStateMap(void *arg)
{
	WriteStateMap = NULL;
	WriteStateContext = NULL;
}


ColumnarWriteState *
columnar_init_write_state(Relation relation, TupleDesc tupdesc,
						  Oid tupSlotRelationId,
						  SubTransactionId currentSubXid)
{
	bool found;

	/*
	 * If this is the first call in current transaction, allocate the hash
	 * table.
	 */
	if (WriteStateMap == NULL)
	{
		WriteStateContext =
			AllocSetContextCreate(
				TopTransactionContext,
				"Column Store Write State Management Context",
				ALLOCSET_DEFAULT_SIZES);
		HASHCTL info;
		uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(RelFileNumber);
		info.hash = oid_hash;
		info.entrysize = sizeof(WriteStateMapEntry);
		info.hcxt = WriteStateContext;

		WriteStateMap = hash_create("column store write state map",
									64, &info, hashFlags);

		cleanupCallback.arg = NULL;
		cleanupCallback.func = &CleanupWriteStateMap;
		cleanupCallback.next = NULL;
		MemoryContextRegisterResetCallback(WriteStateContext, &cleanupCallback);
	}

	WriteStateMapEntry *hashEntry = hash_search(WriteStateMap,
												&RelationPhysicalIdentifierNumber_compat(
													RelationPhysicalIdentifier_compat(
														relation)),
												HASH_ENTER, &found);
	if (!found)
	{
		hashEntry->writeStateStack = NULL;
		hashEntry->dropped = false;
	}

	Assert(!hashEntry->dropped);

	/*
	 * If top of stack belongs to the current subtransaction, return its
	 * writeState, ...
	 */
	if (hashEntry->writeStateStack != NULL)
	{
		SubXidWriteState *stackHead = hashEntry->writeStateStack;

		if (stackHead->subXid == currentSubXid)
		{
			return stackHead->writeState;
		}
	}

	/*
	 * ... otherwise we need to create a new stack entry for the current
	 * subtransaction.
	 */
	MemoryContext oldContext = MemoryContextSwitchTo(WriteStateContext);

	ColumnarOptions columnarOptions = { 0 };

	/*
	 * In case of a table rewrite, we need to fetch table options based on the
	 * relation id of the source tuple slot.
	 *
	 * For this reason, we always pass tupSlotRelationId here; which should be
	 * same as the target table if the write operation is not related to a table
	 * rewrite etc.
	 */
	ReadColumnarOptions(tupSlotRelationId, &columnarOptions);

	SubXidWriteState *stackEntry = palloc0(sizeof(SubXidWriteState));
	stackEntry->writeState = ColumnarBeginWrite(RelationPhysicalIdentifier_compat(
													relation),
												columnarOptions,
												tupdesc);
	stackEntry->subXid = currentSubXid;
	stackEntry->next = hashEntry->writeStateStack;
	hashEntry->writeStateStack = stackEntry;

	MemoryContextSwitchTo(oldContext);

	return stackEntry->writeState;
}


/*
 * Flushes pending writes for given relfilenode in the given subtransaction.
 */
void
FlushWriteStateForRelfilenumber(RelFileNumber relfilenumber,
								SubTransactionId currentSubXid)
{
	if (WriteStateMap == NULL)
	{
		return;
	}

	WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenumber, HASH_FIND,
											NULL);

	Assert(!entry || !entry->dropped);

	if (entry && entry->writeStateStack != NULL)
	{
		SubXidWriteState *stackEntry = entry->writeStateStack;
		if (stackEntry->subXid == currentSubXid)
		{
			ColumnarFlushPendingWrites(stackEntry->writeState);
		}
	}
}


/*
 * Helper function for FlushWriteStateForAllRels and DiscardWriteStateForAllRels.
 * Pops all of write states for current subtransaction, and depending on "commit"
 * either flushes them or discards them. This also takes into account dropped
 * tables, and either propagates the dropped flag to parent subtransaction or
 * rolls back abort.
 */
static void
PopWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid,
						bool commit)
{
	HASH_SEQ_STATUS status;
	WriteStateMapEntry *entry;

	if (WriteStateMap == NULL)
	{
		return;
	}

	hash_seq_init(&status, WriteStateMap);
	while ((entry = hash_seq_search(&status)) != 0)
	{
		if (entry->writeStateStack == NULL)
		{
			continue;
		}

		/*
		 * If the table has been dropped in current subtransaction, either
		 * commit the drop or roll it back.
		 */
		if (entry->dropped)
		{
			if (entry->dropSubXid == currentSubXid)
			{
				if (commit)
				{
					/* elevate drop to the upper subtransaction */
					entry->dropSubXid = parentSubXid;
				}
				else
				{
					/* abort the drop */
					entry->dropped = false;
				}
			}
		}

		/*
		 * Otherwise, commit or discard pending writes.
		 */
		else
		{
			SubXidWriteState *stackHead = entry->writeStateStack;
			if (stackHead->subXid == currentSubXid)
			{
				if (commit)
				{
					ColumnarEndWrite(stackHead->writeState);
				}

				entry->writeStateStack = stackHead->next;
			}
		}
	}
}


/*
 * Called when current subtransaction is committed.
 */
void
FlushWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid)
{
	PopWriteStateForAllRels(currentSubXid, parentSubXid, true);
}


/*
 * Called when current subtransaction is aborted.
 */
void
DiscardWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid)
{
	PopWriteStateForAllRels(currentSubXid, parentSubXid, false);
}


/*
 * Called when the given relfilenode is dropped.
 */
void
MarkRelfilenumberDropped(RelFileNumber relfilenumber, SubTransactionId currentSubXid)
{
	if (WriteStateMap == NULL)
	{
		return;
	}

	WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenumber, HASH_FIND,
											NULL);
	if (!entry || entry->dropped)
	{
		return;
	}

	entry->dropped = true;
	entry->dropSubXid = currentSubXid;
}


/*
 * Called when the given relfilenode is dropped in non-transactional TRUNCATE.
 */
void
NonTransactionDropWriteState(RelFileNumber relfilenumber)
{
	if (WriteStateMap)
	{
		hash_search(WriteStateMap, &relfilenumber, HASH_REMOVE, false);
	}
}


/*
 * Returns true if there are any pending writes in upper transactions.
 */
bool
PendingWritesInUpperTransactions(RelFileNumber relfilenumber,
								 SubTransactionId currentSubXid)
{
	if (WriteStateMap == NULL)
	{
		return false;
	}

	WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenumber, HASH_FIND,
											NULL);

	if (entry && entry->writeStateStack != NULL)
	{
		SubXidWriteState *stackEntry = entry->writeStateStack;

		while (stackEntry != NULL)
		{
			if (stackEntry->subXid != currentSubXid &&
				ContainsPendingWrites(stackEntry->writeState))
			{
				return true;
			}

			stackEntry = stackEntry->next;
		}
	}

	return false;
}


/*
 * GetWriteContextForDebug exposes WriteStateContext for debugging
 * purposes.
 */
extern MemoryContext
GetWriteContextForDebug(void)
{
	return WriteStateContext;
}
