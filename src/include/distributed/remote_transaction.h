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
	bool criticalTransaction;

	/* failed in current transaction */
	bool transactionFailed;

	/* 2PC transaction name currently associated with connection */
	char preparedName[NAMEDATALEN];
} RemoteTransaction;


/* change remote transaction state */
extern void AdjustRemoteTransactionState(struct MultiConnection *connection);
extern void AdjustRemoteTransactionStates(List *connectionList);
extern void MarkRemoteTransactionFailed(struct MultiConnection *connection,
										bool allowErrorPromotion);

extern void CoordinatedRemoteTransactionsCommit(void);
extern void CoordinatedRemoteTransactionsAbort(void);
extern void CoordinatedRemoteTransactionsPrepare(void);

#endif /* REMOTE_TRANSACTION_H */
