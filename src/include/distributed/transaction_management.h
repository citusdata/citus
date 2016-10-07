/*-------------------------------------------------------------------------
 * transaction_management.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_MANAGMENT_H
#define TRANSACTION_MANAGMENT_H

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
	COORD_TRANS_IDLE
} CoordinatedTransactionState;


/* Enumeration that defines the different commit protocols available */
typedef enum
{
	COMMIT_PROTOCOL_1PC = 0,
	COMMIT_PROTOCOL_2PC = 1
} CommitProtocolType;

/* config variable managed via guc.c */
extern int MultiShardCommitProtocol;

/* state needed to prevent new connections during modifying transactions */
extern XactModificationType XactModificationLevel;


extern CoordinatedTransactionState CurrentCoordinatedTransactionState;


/*
 * Initialization.
 */
extern void InitializeTransactionManagement(void);


#endif /*  TRANSACTION_MANAGMENT_H */
