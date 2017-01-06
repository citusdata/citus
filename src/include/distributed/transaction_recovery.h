/*-------------------------------------------------------------------------
 *
 * transaction_recovery.h
 *	  Type and function declarations used in recovering 2PC transactions.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_RECOVERY_H
#define TRANSACTION_RECOVERY_H


/* Functions declarations for worker transactions */
extern void LogTransactionRecord(int groupId, char *transactionName);


#endif /* TRANSACTION_RECOVERY_H */
