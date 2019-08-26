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

#include "distributed/commands.h"
#include "nodes/parsenodes.h"


/* placeholder for ProcessCreateSubscriptionStmt */
Node *
ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt)
{
	return (Node *) createSubStmt;
}
