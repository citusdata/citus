/*-------------------------------------------------------------------------
 * remote_transaction.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#ifndef REMOTE_TRANSACTION_H
#define REMOTE_TRANSACTION_H


#include "libpq-fe.h"
#include "nodes/pg_list.h"
#include "lib/ilist.h"

/*
 * Not to exceed the community-code global transaction
 * identifier (GID) size (200)
 */
#define PREPARED_TRANSACTION_NAME_LEN 128

/* forward declare, to avoid recursive includes */
struct MultiConnection;

/*
 * Enum that defines different remote transaction states, of a single remote
 * transaction.
 */
typedef enum
{
	/* no transaction active */
	REMOTE_TRANS_NOT_STARTED = 0,

	/* transaction start */
	REMOTE_TRANS_STARTING,
	REMOTE_TRANS_STARTED,

	/* command execution */
	REMOTE_TRANS_SENT_BEGIN,
	REMOTE_TRANS_SENT_COMMAND,
	REMOTE_TRANS_FETCHING_RESULTS,
	REMOTE_TRANS_CLEARING_RESULTS,

	/* 2pc prepare */
	REMOTE_TRANS_PREPARING,
	REMOTE_TRANS_PREPARED,

	/* transaction abort */
	REMOTE_TRANS_1PC_ABORTING,
	REMOTE_TRANS_2PC_ABORTING,
	REMOTE_TRANS_ABORTED,

	/* transaction commit */
	REMOTE_TRANS_1PC_COMMITTING,
	REMOTE_TRANS_2PC_COMMITTING,
	REMOTE_TRANS_COMMITTED
} RemoteTransactionState;


/*
 * Transaction state associated associated with a single MultiConnection.
 */
typedef struct RemoteTransaction
{
	/* what state is the remote side transaction in */
	RemoteTransactionState transactionState;

	/* failures on this connection should abort entire coordinated transaction */
	bool transactionCritical;

	/* failed in current transaction */
	bool transactionFailed;

	/*
	 * Id of last savepoint that successfully began before transaction failure.
	 * Since savepoint ids are assigned incrementally, rolling back to any savepoint
	 * with id equal to or less than this id recovers the transaction from failures.
	 */
	SubTransactionId lastSuccessfulSubXact;

	/* Id of last savepoint queued before first query of transaction */
	SubTransactionId lastQueuedSubXact;

	/* waiting for the result of a recovering ROLLBACK TO SAVEPOINT command */
	bool transactionRecovering;

	/* 2PC transaction name currently associated with connection */
	char preparedName[PREPARED_TRANSACTION_NAME_LEN];

	/* set when BEGIN is sent over the connection */
	bool beginSent;
} RemoteTransaction;

extern bool EnableGlobalClock;


/* utility functions for dealing with remote transactions */
extern bool ParsePreparedTransactionName(char *preparedTransactionName, int32 *groupId,
										 int *procId, uint64 *transactionNumber,
										 uint32 *connectionNumber,
										 uint64 *transactionClockValue);

/* change an individual remote transaction's state */
extern void StartRemoteTransactionBegin(struct MultiConnection *connection);
extern void FinishRemoteTransactionBegin(struct MultiConnection *connection);
extern void RemoteTransactionBegin(struct MultiConnection *connection);
extern void RemoteTransactionListBegin(List *connectionList);

extern void StartRemoteTransactionPrepare(struct MultiConnection *connection);
extern void FinishRemoteTransactionPrepare(struct MultiConnection *connection);

extern void StartRemoteTransactionCommit(struct MultiConnection *connection);
extern void FinishRemoteTransactionCommit(struct MultiConnection *connection);
extern void RemoteTransactionCommit(struct MultiConnection *connection);

extern void StartRemoteTransactionAbort(struct MultiConnection *connection);
extern void FinishRemoteTransactionAbort(struct MultiConnection *connection);
extern void RemoteTransactionAbort(struct MultiConnection *connection);

/* start transaction if necessary */
extern void RemoteTransactionBeginIfNecessary(struct MultiConnection *connection);
extern void RemoteTransactionsBeginIfNecessary(List *connectionList);

/* other public functionality */
extern void HandleRemoteTransactionConnectionError(struct MultiConnection *connection,
												   bool raiseError);
extern void HandleRemoteTransactionResultError(struct MultiConnection *connection,
											   PGresult *result, bool raiseErrors);
extern void MarkRemoteTransactionFailed(struct MultiConnection *connection,
										bool allowErrorPromotion);
extern void MarkRemoteTransactionCritical(struct MultiConnection *connection);


/*
 * The following functions should all only be called by connection /
 * transaction managment code.
 */

extern void CloseRemoteTransaction(struct MultiConnection *connection);
extern void ResetRemoteTransaction(struct MultiConnection *connection);

/* perform handling for all in-progress transactions */
extern void CoordinatedRemoteTransactionsPrepare(void);
extern void CoordinatedRemoteTransactionsCommit(void);
extern void CoordinatedRemoteTransactionsAbort(void);
extern void CheckRemoteTransactionsHealth(void);

/* remote savepoint commands */
extern void CoordinatedRemoteTransactionsSavepointBegin(SubTransactionId subId);
extern void CoordinatedRemoteTransactionsSavepointRelease(SubTransactionId subId);
extern void CoordinatedRemoteTransactionsSavepointRollback(SubTransactionId subId);

#endif /* REMOTE_TRANSACTION_H */
