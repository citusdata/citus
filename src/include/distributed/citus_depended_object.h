/*-------------------------------------------------------------------------
 *
 * citus_depended_object.h
 *   Exposes functions related to hiding citus depended objects while executing
 *   postgres vanilla tests.
 *
 * Copyright (c) CitusDependent Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPENDED_OBJECT_H
#define CITUS_DEPENDED_OBJECT_H

#include "distributed/commands.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

extern bool HideCitusDependentObjects;

extern void SetLocalClientMinMessagesIfRunningPGTests(int
													  clientMinMessageLevel);
extern void SetLocalHideCitusDependentObjectsDisabledWhenAlreadyEnabled(void);
extern bool HideCitusDependentObjectsOnQueriesOfPgMetaTables(Node *node, void *context);
extern bool IsPgLocksTable(RangeTblEntry *rte);
extern bool DistOpsHasInvalidObject(Node *node, const DistributeObjectOps *ops);

#endif /* CITUS_DEPENDED_OBJECT_H */
