/*-------------------------------------------------------------------------
 * remote_transaction.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#ifndef REMOTE_TRANSACTION_H
#define REMOTE_TRANSACTION_H


#include "nodes/pg_list.h"
#include "lib/ilist.h"


/* forward declare, to avoid recursive includes */
struct MultiConnection;

/*
 * Enum that defines different remote transaction states, of a single remote
 * transaction.
 */
typedef enum
{
	/* no transaction active */
	REMOTE_TRANS_INVALID = 0,

	/* transaction start */
	REMOTE_TRANS_STARTING,
	REMOTE_TRANS_STARTED,

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

	/* 2PC transaction name currently associated with connection */
	char preparedName[NAMEDATALEN];
} RemoteTransaction;


/* change an individual remote transaction's state */
extern void StartRemoteTransactionBegin(struct MultiConnection *connection);
extern void FinishRemoteTransactionBegin(struct MultiConnection *connection);
extern void RemoteTransactionBegin(struct MultiConnection *connection);
extern void RemoteTransactionListBegin(List *connectionList);

extern void StartRemoteTransactionPrepare(struct MultiConnection *connection);
extern void FinishRemoteTransactionPrepare(struct MultiConnection *connection);
extern void RemoteTransactionPrepare(struct MultiConnection *connection);

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

#endif /* REMOTE_TRANSACTION_H */
