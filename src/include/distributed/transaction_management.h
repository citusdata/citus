/*-------------------------------------------------------------------------
 * transaction_management.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_MANAGMENT_H
#define TRANSACTION_MANAGMENT_H

#include "access/xact.h"
#include "catalog/objectaddress.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/hsearch.h"

/* forward declare, to avoid recursive includes */
struct DistObjectCacheEntry;


/* describes what kind of modifications have occurred in the current transaction */
typedef enum
{
	XACT_MODIFICATION_INVALID = 0, /* placeholder initial value */
	XACT_MODIFICATION_NONE,        /* no modifications have taken place */
	XACT_MODIFICATION_DATA,        /* data modifications (DML) have occurred */
	XACT_MODIFICATION_MULTI_SHARD  /* multi-shard modifications have occurred */
} XactModificationType;


/*
 * Enum defining the state of a coordinated (i.e. a transaction potentially
 * spanning several nodes).
 */
typedef enum CoordinatedTransactionState
{
	/* no coordinated transaction in progress, no connections established */
	COORD_TRANS_NONE,

	/* no coordinated transaction in progress, but connections established */
	COORD_TRANS_IDLE,

	/* coordinated transaction in progress */
	COORD_TRANS_STARTED,

	/* coordinated transaction prepared on all workers */
	COORD_TRANS_PREPARED,

	/* coordinated transaction committed */
	COORD_TRANS_COMMITTED
} CoordinatedTransactionState;


/* Enumeration to keep track of context within nested sub-transactions */
typedef struct SubXactContext
{
	SubTransactionId subId;
	StringInfo setLocalCmds;
	HTAB *propagatedObjects;
} SubXactContext;

/*
 * Function delegated with force_delegation call enforces the distribution argument
 * along with the colocationId. The latter one is equally important to not allow
 * the same partition key value into another distributed table which is not co-located
 * and therefore might be on a different node.
 */
typedef struct AllowedDistributionColumn
{
	Const *distributionColumnValue;
	uint32 colocationId;
	bool isActive;

	/* In nested executor, track the level at which value is set */
	int executorLevel;
} AllowedDistributionColumn;

/*
 * BeginXactDeferrableState reflects the value of the DEFERRABLE property
 * in the BEGIN of a transaction block.
 */
typedef enum BeginXactDeferrableState
{
	BeginXactDeferrable_NotSet,
	BeginXactDeferrable_Disabled,
	BeginXactDeferrable_Enabled,
} BeginXactDeferrableState;

/*
 * BeginXactReadOnlyState reflects the value of the READ ONLY property
 * in the BEGIN of a transaction block.
 */
typedef enum BeginXactReadOnlyState
{
	BeginXactReadOnly_NotSet,
	BeginXactReadOnly_Disabled,
	BeginXactReadOnly_Enabled,
} BeginXactReadOnlyState;

/*
 * The current distribution column value passed as an argument to a forced
 * function call delegation.
 */
extern AllowedDistributionColumn AllowedDistributionColumnValue;

/*
 * GUC that determines whether a SELECT in a transaction block should also run in
 * a transaction block on the worker.
 */
extern bool SelectOpensTransactionBlock;

/*
 * GUC that determines whether a function should be considered a transaction
 * block.
 */
extern bool FunctionOpensTransactionBlock;

/* state needed to prevent new connections during modifying transactions */
extern XactModificationType XactModificationLevel;

extern CoordinatedTransactionState CurrentCoordinatedTransactionState;

/* list of connections that are part of the current coordinated transaction */
extern dlist_head InProgressTransactions;

/* controls use of locks to enforce safe commutativity */
extern bool AllModificationsCommutative;

/* we've deprecated this flag, keeping here for some time not to break existing users */
extern bool EnableDeadlockPrevention;

/* number of nested stored procedure call levels we are currently in */
extern int StoredProcedureLevel;

/* number of nested DO block levels we are currently in */
extern int DoBlockLevel;

/* SET LOCAL statements active in the current (sub-)transaction. */
extern StringInfo activeSetStmts;

/* did current transaction modify pg_dist_node? */
extern bool TransactionModifiedNodeMetadata;

/* after an explicit BEGIN, keep track of top-level transaction characteristics */
extern BeginXactReadOnlyState BeginXactReadOnly;
extern BeginXactDeferrableState BeginXactDeferrable;

/*
 * Coordinated transaction management.
 */
extern void UseCoordinatedTransaction(void);
extern bool InCoordinatedTransaction(void);
extern void Use2PCForCoordinatedTransaction(void);
extern bool GetCoordinatedTransactionShouldUse2PC(void);
extern bool IsMultiStatementTransaction(void);
extern void EnsureDistributedTransactionId(void);
extern bool MaybeExecutingUDF(void);

/* functions for tracking the objects propagated in current transaction */
extern void TrackPropagatedObject(const ObjectAddress *objectAddress);
extern void TrackPropagatedTableAndSequences(Oid relationId);
extern void ResetPropagatedObjects(void);
extern bool HasAnyDependencyInPropagatedObjects(const ObjectAddress *objectAddress);

/* initialization function(s) */
extern void InitializeTransactionManagement(void);

/* other functions */
extern List * ActiveSubXactContexts(void);
extern StringInfo BeginAndSetDistributedTransactionIdCommand(void);
extern void TriggerNodeMetadataSyncOnCommit(void);


#endif /*  TRANSACTION_MANAGMENT_H */
