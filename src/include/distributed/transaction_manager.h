/*-------------------------------------------------------------------------
 *
 * transaction_manager.h
 *
 * Transaction manager API.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H


#include "libpq-fe.h"
#include "utils/guc.h"


/* pluggable transaction manager API */
typedef struct CitusTransactionManager
{
	bool (*Begin)(PGconn *conn);
	bool (*Prepare)(PGconn *conn, char *transactionId);
	bool (*CommitPrepared)(PGconn *conn, char *transactionId);
	bool (*RollbackPrepared)(PGconn *conn, char *transactionId);
	bool (*Rollback)(PGconn *conn);
} CitusTransactionManager;


/* Enumeration that defines the transaction manager to use */
typedef enum
{
	TRANSACTION_MANAGER_1PC = 0,
	TRANSACTION_MANAGER_2PC = 1
} TransactionManagerType;


/* Implementations of the transaction manager API */
extern CitusTransactionManager const CitusTransactionManagerImpl[];


/* Function declarations for copying into a distributed table */
extern bool ExecuteCommand(PGconn *connection, ExecStatusType expectedResult,
						   char const *command);
extern char * BuildTransactionId(int localId);


#endif /* TRANSACTION_MANAGER_H */
