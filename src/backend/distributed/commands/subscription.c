/*-------------------------------------------------------------------------
 *
 * subscription.c
 *    Commands for creating subscriptions
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if (PG_VERSION_NUM >= 100000)

#include "distributed/commands.h"
#include "nodes/parsenodes.h"


/* placeholder for ProcessCreateSubscriptionStmt */
Node *
ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt)
{
	return (Node *) createSubStmt;
}


#endif /* PG_VERSION_NUM >= 100000 */
