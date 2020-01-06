/*
 * transaction_identifier.h
 *
 *    Data structure for distributed transaction id and related function
 *    declarations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef TRANSACTION_IDENTIFIER_H
#define TRANSACTION_IDENTIFIER_H


#include "datatype/timestamp.h"


/*
 * Citus identifies a distributed transaction with a triplet consisting of
 *
 *  -  initiatorNodeIdentifier: A unique identifier of the node that initiated
 *     the distributed transaction
 *  -  transactionOriginator: Set to true only for the transactions initialized on
 *     the coordinator. This is only useful for MX in order to distinguish the transaction
 *     that started the distributed transaction on the coordinator where we could
 *     have the same transactions' worker queries on the same node
 *  -  transactionNumber: A locally unique identifier assigned for the distributed
 *     transaction on the node that initiated the distributed transaction
 *  -  timestamp: The current timestamp of distributed transaction initiation
 *
 */
typedef struct DistributedTransactionId
{
	int initiatorNodeIdentifier;
	bool transactionOriginator;
	uint64 transactionNumber;
	TimestampTz timestamp;
} DistributedTransactionId;


extern DistributedTransactionId * GetCurrentDistributedTransactionId(void);
extern uint64 CurrentDistributedTransactionNumber(void);

#endif /* TRANSACTION_IDENTIFIER_H */
