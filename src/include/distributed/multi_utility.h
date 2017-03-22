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

typedef struct DDLJob
{
	Oid targetRelationId;
	const char *commandString;
	List *taskList;
} DDLJob;

extern void multi_ProcessUtility(Node *parsetree, const char *queryString,
								 ProcessUtilityContext context, ParamListInfo params,
								 DestReceiver *dest, char *completionTag);
extern void ReplicateGrantStmt(Node *parsetree);
extern void ErrorIfNotSupportedConstraint(Relation relation, char distributionMethod,
										  Var *distributionColumn, uint32 colocationId);
extern void ErrorIfNotSupportedForeignConstraint(Relation relation,
												 char distributionMethod,
												 Var *distributionColumn,
												 uint32 colocationId);

#endif /* MULTI_UTILITY_H */
