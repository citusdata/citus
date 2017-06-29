/*-------------------------------------------------------------------------
 * transaction_management.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_MANAGMENT_H
#define TRANSACTION_MANAGMENT_H

#include "lib/ilist.h"

#include "datatype/timestamp.h"
#include "storage/lwlock.h"

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


/* Enumeration that defines the different commit protocols available */
typedef enum
{
	COMMIT_PROTOCOL_BARE = 0,
	COMMIT_PROTOCOL_1PC = 1,
	COMMIT_PROTOCOL_2PC = 2
} CommitProtocolType;


/* FIXME: move to different header? */
typedef struct TmgmtTransactionId
{
	uint64 nodeId;
	uint64 transactionId;
	TimestampTz timestamp;
} TmgmtTransactionId;

typedef struct TmgmtBackendData
{
	Oid databaseId;
	TmgmtTransactionId transactionId;
} TmgmtBackendData;


typedef struct TmgmtShmemControlData
{
	int trancheId;
	LWLockTranche lockTranche;
	LWLock lock;

	int numSessions;

	pg_atomic_uint64 nextTransactionId;

	TmgmtBackendData sessions[FLEXIBLE_ARRAY_MEMBER];
} TmgmtShmemControlData;

extern TmgmtBackendData *MyTmgmtBackendData;
extern TmgmtShmemControlData *TmgmtShmemControl;


/* config variable managed via guc.c */
extern int MultiShardCommitProtocol;

/* state needed to restore multi-shard commit protocol during VACUUM/ANALYZE */
extern int SavedMultiShardCommitProtocol;

/* state needed to prevent new connections during modifying transactions */
extern XactModificationType XactModificationLevel;

extern CoordinatedTransactionState CurrentCoordinatedTransactionState;

/* list of connections that are part of the current coordinated transaction */
extern dlist_head InProgressTransactions;

/*
 * Coordinated transaction management.
 */
extern void BeginCoordinatedTransaction(void);
extern void BeginOrContinueCoordinatedTransaction(void);
extern bool InCoordinatedTransaction(void);
extern void CoordinatedTransactionUse2PC(void);

/* initialization function(s) */
extern void InitializeTransactionManagement(void);
extern void InitializeTransactionManagementBackend(void);


#endif /*  TRANSACTION_MANAGMENT_H */
