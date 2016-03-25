/*-------------------------------------------------------------------------
 *
 * multi_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_TRANSACTION_H
#define MULTI_TRANSACTION_H


#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* Enumeration that defines the different transaction managers available */
typedef enum
{
	TRANSACTION_MANAGER_1PC = 0,
	TRANSACTION_MANAGER_2PC = 1
} TransactionManagerType;

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
	int64 connectionId;
	TransactionState transactionState;
	PGconn *connection;
} TransactionConnection;


/* Functions declarations for transaction and connection management */
extern void PrepareRemoteTransactions(List *connectionList);
extern void AbortRemoteTransactions(List *connectionList);
extern void CommitRemoteTransactions(List *connectionList);
extern void CloseConnections(List *connectionList);


#endif /* MULTI_TRANSACTION_H */
