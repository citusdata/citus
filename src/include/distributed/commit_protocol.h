/*-------------------------------------------------------------------------
 *
 * commit_protocol.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMMIT_PROTOCOL_H
#define COMMIT_PROTOCOL_H


#include "access/xact.h"
#include "distributed/connection_management.h"
#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* Enumeration that defines different remote transaction states */
typedef enum
{
	TRANSACTION_STATE_INVALID = 0,
	TRANSACTION_STATE_OPEN,
	TRANSACTION_STATE_COPY_STARTED,
	TRANSACTION_STATE_PREPARED,
	TRANSACTION_STATE_CLOSED
} TransactionState;

/*
 * TransactionConnection represents a connection to a remote node which is
 * used to perform a transaction on shard placements.
 */
typedef struct TransactionConnection
{
	int groupId;
	int64 connectionId;
	TransactionState transactionState;
	PGconn *connection;
	const char *nodeName;
	int nodePort;
} TransactionConnection;


/* Functions declarations for transaction and connection management */
extern void InitializeDistributedTransaction(void);
extern void PrepareRemoteTransactions(List *connectionList);
extern void AbortRemoteTransactions(List *connectionList);
extern void CommitRemoteTransactions(List *connectionList, bool stopOnFailure);
extern StringInfo BuildTransactionName(int connectionId);

#endif /* COMMIT_PROTOCOL_H */
