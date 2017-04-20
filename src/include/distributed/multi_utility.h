/*-------------------------------------------------------------------------
 *
 * multi_utility.h
 *	  Citus utility hook and related functionality.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_UTILITY_H
#define MULTI_UTILITY_H

#include "tcop/utility.h"

extern bool EnableDDLPropagation;
extern bool EnableVersionChecks;

/*
 * A DDLJob encapsulates the remote tasks and commands needed to process all or
 * part of a distributed DDL command. It hold the distributed relation's oid,
 * the original DDL command string (for MX DDL propagation), and a task list of
 * DDL_TASK-type Tasks to be executed.
 */
typedef struct DDLJob
{
	Oid targetRelationId;      /* oid of the target distributed relation */
	bool concurrentIndexCmd;   /* related to a CONCURRENTLY index command? */
	const char *commandString; /* initial (coordinator) DDL command string */
	List *taskList;            /* worker DDL tasks to execute */
} DDLJob;

extern void multi_ProcessUtility(Node *parsetree, const char *queryString,
								 ProcessUtilityContext context, ParamListInfo params,
								 DestReceiver *dest, char *completionTag);
extern List * PlanGrantStmt(GrantStmt *grantStmt);
extern void ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
										 Var *distributionColumn, uint32 colocationId);


#endif /* MULTI_UTILITY_H */
