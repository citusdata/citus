/*-------------------------------------------------------------------------
 *
 * citus_outfuncs.c
 *	  Output functions for Citus tree nodes.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * NOTES
 *	  This is a wrapper around postgres' nodeToString() that additionally
 *	  supports Citus node types.
 *
 *    Keep as closely aligned with the upstream version as possible.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/master_metadata_utility.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"


/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Store const reference to raw input node in local named 'node' */
#define WRITE_LOCALS(nodeTypeName) \
		const nodeTypeName *node = (const nodeTypeName *) raw_node

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	(void) 0

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an 64-bit integer field (anything written as ":fldname %d") */
#define WRITE_INT64_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " " INT64_FORMAT, node->fldname)


/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* XXX: Citus: Write an unsigned 64-bit integer field */
#define WRITE_UINT64_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " " UINT64_FORMAT, node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %c", node->fldname)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 _outBitmapset(str, node->fldname))


/* Write an integer array (anything written as ":fldname (%d, %d") */
#define WRITE_INT_ARRAY(fldname, count) \
	appendStringInfo(str, " :" CppAsString(fldname) " ("); \
	{ \
		int i;\
		for (i = 0; i < count; i++) \
		{ \
			if (i > 0) \
			{ \
				appendStringInfo(str, ", "); \
			} \
			appendStringInfo(str, "%d", node->fldname[i]); \
		}\
	}\
	appendStringInfo(str, ")")


/* Write an enum array (anything written as ":fldname (%d, %d") */
#define WRITE_ENUM_ARRAY(fldname, count) WRITE_INT_ARRAY(fldname, count)


#define booltostr(x)  ((x) ? "true" : "false")


/*****************************************************************************
 *	Output routines for Citus node types
 *****************************************************************************/

static void
OutMultiUnaryNodeFields(StringInfo str, const MultiUnaryNode *node)
{
	WRITE_NODE_FIELD(childNode);
}


static void
OutMultiBinaryNodeFields(StringInfo str, const MultiBinaryNode *node)
{
	WRITE_NODE_FIELD(leftChildNode);
	WRITE_NODE_FIELD(rightChildNode);
}

void
OutMultiNode(OUTFUNC_ARGS)
{
	WRITE_NODE_TYPE("MULTINODE");
}


void
OutMultiTreeRoot(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiTreeRoot);

	WRITE_NODE_TYPE("MULTITREEROOT");

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutDistributedPlan(OUTFUNC_ARGS)
{
	WRITE_LOCALS(DistributedPlan);

	WRITE_NODE_TYPE("DISTRIBUTEDPLAN");

	WRITE_UINT64_FIELD(planId);
	WRITE_INT_FIELD(operation);
	WRITE_BOOL_FIELD(hasReturning);

	WRITE_NODE_FIELD(workerJob);
	WRITE_NODE_FIELD(masterQuery);
	WRITE_BOOL_FIELD(routerExecutable);
	WRITE_UINT64_FIELD(queryId);
	WRITE_NODE_FIELD(relationIdList);

	WRITE_NODE_FIELD(insertSelectSubquery);
	WRITE_NODE_FIELD(insertTargetList);
	WRITE_OID_FIELD(targetRelationId);
	WRITE_STRING_FIELD(intermediateResultIdPrefix);

	WRITE_NODE_FIELD(subPlanList);

	WRITE_NODE_FIELD(planningError);
}


void
OutDistributedSubPlan(OUTFUNC_ARGS)
{
	WRITE_LOCALS(DistributedSubPlan);

	WRITE_NODE_TYPE("DISTRIBUTEDSUBPLAN");

	WRITE_UINT_FIELD(subPlanId);
	WRITE_NODE_FIELD(plan);
}


void
OutMultiProject(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiProject);
	WRITE_NODE_TYPE("MULTIPROJECT");

	WRITE_NODE_FIELD(columnList);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiCollect(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiCollect);
	WRITE_NODE_TYPE("MULTICOLLECT");

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiSelect(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiSelect);
	WRITE_NODE_TYPE("MULTISELECT");

	WRITE_NODE_FIELD(selectClauseList);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiTable(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiTable);
	WRITE_NODE_TYPE("MULTITABLE");

	WRITE_OID_FIELD(relationId);
	WRITE_INT_FIELD(rangeTableId);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiJoin(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiJoin);
	WRITE_NODE_TYPE("MULTIJOIN");

	WRITE_NODE_FIELD(joinClauseList);
	WRITE_ENUM_FIELD(joinRuleType, JoinRuleType);
	WRITE_ENUM_FIELD(joinType, JoinType);

	OutMultiBinaryNodeFields(str, (const MultiBinaryNode *) node);
}


void
OutMultiPartition(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiPartition);
	WRITE_NODE_TYPE("MULTIPARTITION");

	WRITE_NODE_FIELD(partitionColumn);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiCartesianProduct(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiCartesianProduct);
	WRITE_NODE_TYPE("MULTICARTESIANPRODUCT");

	OutMultiBinaryNodeFields(str, (const MultiBinaryNode *) node);
}




void
OutMultiExtendedOp(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiExtendedOp);
	WRITE_NODE_TYPE("MULTIEXTENDEDOP");

	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(groupClauseList);
	WRITE_NODE_FIELD(sortClauseList);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(havingQual);
	WRITE_BOOL_FIELD(hasDistinctOn);
	WRITE_NODE_FIELD(distinctClause);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}

static void
OutJobFields(StringInfo str, const Job *node)
{
	WRITE_UINT64_FIELD(jobId);
	WRITE_NODE_FIELD(jobQuery);
	WRITE_NODE_FIELD(taskList);
	WRITE_NODE_FIELD(dependedJobList);
	WRITE_BOOL_FIELD(subqueryPushdown);
	WRITE_BOOL_FIELD(requiresMasterEvaluation);
	WRITE_BOOL_FIELD(deferredPruning);
	WRITE_NODE_FIELD(partitionKeyValue);
}


void
OutJob(OUTFUNC_ARGS)
{
	WRITE_LOCALS(Job);
	WRITE_NODE_TYPE("JOB");

	OutJobFields(str, node);
}


void
OutShardInterval(OUTFUNC_ARGS)
{
	WRITE_LOCALS(ShardInterval);
	WRITE_NODE_TYPE("SHARDINTERVAL");

	WRITE_OID_FIELD(relationId);
	WRITE_CHAR_FIELD(storageType);
	WRITE_OID_FIELD(valueTypeId);
	WRITE_INT_FIELD(valueTypeLen);
	WRITE_BOOL_FIELD(valueByVal);
	WRITE_BOOL_FIELD(minValueExists);
	WRITE_BOOL_FIELD(maxValueExists);

	appendStringInfoString(str, " :minValue ");
	if (!node->minValueExists)
		appendStringInfoString(str, "<>");
	else
		outDatum(str, node->minValue, node->valueTypeLen, node->valueByVal);

	appendStringInfoString(str, " :maxValue ");
	if (!node->maxValueExists)
		appendStringInfoString(str, "<>");
	else
		outDatum(str, node->maxValue, node->valueTypeLen, node->valueByVal);

	WRITE_UINT64_FIELD(shardId);
	WRITE_INT_FIELD(shardIndex);
}


void
OutMapMergeJob(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MapMergeJob);
	int arrayLength = node->sortedShardIntervalArrayLength;
	int i;

	WRITE_NODE_TYPE("MAPMERGEJOB");

	OutJobFields(str, (Job *) node);
	WRITE_NODE_FIELD(reduceQuery);
	WRITE_ENUM_FIELD(partitionType, PartitionType);
	WRITE_NODE_FIELD(partitionColumn);
	WRITE_UINT_FIELD(partitionCount);
	WRITE_INT_FIELD(sortedShardIntervalArrayLength);

	for (i = 0; i < arrayLength; ++i)
	{
		outNode(str, node->sortedShardIntervalArray[i]);
	}

	WRITE_NODE_FIELD(mapTaskList);
	WRITE_NODE_FIELD(mergeTaskList);
}


void
OutShardPlacement(OUTFUNC_ARGS)
{
	WRITE_LOCALS(ShardPlacement);
	WRITE_NODE_TYPE("SHARDPLACEMENT");

	WRITE_UINT64_FIELD(placementId);
	WRITE_UINT64_FIELD(shardId);
	WRITE_UINT64_FIELD(shardLength);
	WRITE_ENUM_FIELD(shardState, RelayFileState);
	WRITE_INT_FIELD(groupId);
	WRITE_STRING_FIELD(nodeName);
	WRITE_UINT_FIELD(nodePort);
	WRITE_INT_FIELD(nodeId);
	/* so we can deal with 0 */
	WRITE_INT_FIELD(partitionMethod);
	WRITE_UINT_FIELD(colocationGroupId);
	WRITE_UINT_FIELD(representativeValue);
}


void
OutGroupShardPlacement(OUTFUNC_ARGS)
{
	WRITE_LOCALS(GroupShardPlacement);
	WRITE_NODE_TYPE("GROUPSHARDPLACEMENT");

	WRITE_UINT64_FIELD(placementId);
	WRITE_UINT64_FIELD(shardId);
	WRITE_UINT64_FIELD(shardLength);
	WRITE_ENUM_FIELD(shardState, RelayFileState);
	WRITE_INT_FIELD(groupId);
}


void
OutRelationShard(OUTFUNC_ARGS)
{
	WRITE_LOCALS(RelationShard);
	WRITE_NODE_TYPE("RELATIONSHARD");

	WRITE_OID_FIELD(relationId);
	WRITE_UINT64_FIELD(shardId);
}


void
OutRelationRowLock(OUTFUNC_ARGS)
{
	WRITE_LOCALS(RelationRowLock);
	WRITE_NODE_TYPE("RELATIONROWLOCK");

	WRITE_OID_FIELD(relationId);
	WRITE_ENUM_FIELD(rowLockStrength, LockClauseStrength);
}


void
OutTask(OUTFUNC_ARGS)
{
	WRITE_LOCALS(Task);
	WRITE_NODE_TYPE("TASK");

	WRITE_ENUM_FIELD(taskType, TaskType);
	WRITE_UINT64_FIELD(jobId);
	WRITE_UINT_FIELD(taskId);
	WRITE_STRING_FIELD(queryString);
	WRITE_UINT64_FIELD(anchorShardId);
	WRITE_NODE_FIELD(taskPlacementList);
	WRITE_NODE_FIELD(dependedTaskList);
	WRITE_UINT_FIELD(partitionId);
	WRITE_UINT_FIELD(upstreamTaskId);
	WRITE_NODE_FIELD(shardInterval);
	WRITE_BOOL_FIELD(assignmentConstrained);
	WRITE_NODE_FIELD(taskExecution);
	WRITE_BOOL_FIELD(upsertQuery);
	WRITE_CHAR_FIELD(replicationModel);
	WRITE_BOOL_FIELD(modifyWithSubquery);
	WRITE_NODE_FIELD(relationShardList);
	WRITE_NODE_FIELD(relationRowLockList);
	WRITE_NODE_FIELD(rowValuesLists);
}


void
OutTaskExecution(OUTFUNC_ARGS)
{
	WRITE_LOCALS(TaskExecution);
	WRITE_NODE_TYPE("TASKEXECUTION");

	WRITE_UINT64_FIELD(jobId);
	WRITE_UINT_FIELD(taskId);
	WRITE_UINT_FIELD(nodeCount);

	WRITE_ENUM_ARRAY(taskStatusArray, node->nodeCount);
	WRITE_ENUM_ARRAY(transmitStatusArray, node->nodeCount);
	WRITE_INT_ARRAY(connectionIdArray, node->nodeCount);
	WRITE_INT_ARRAY(fileDescriptorArray, node->nodeCount);

	WRITE_INT64_FIELD(connectStartTime);
	WRITE_UINT_FIELD(currentNodeIndex);
	WRITE_UINT_FIELD(querySourceNodeIndex);
	WRITE_UINT_FIELD(failureCount);
	WRITE_BOOL_FIELD(criticalErrorOccurred);
}


void
OutDeferredErrorMessage(OUTFUNC_ARGS)
{
	WRITE_LOCALS(DeferredErrorMessage);
	WRITE_NODE_TYPE("DEFERREDERRORMESSAGE");

	WRITE_INT_FIELD(code);
	WRITE_STRING_FIELD(message);
	WRITE_STRING_FIELD(detail);
	WRITE_STRING_FIELD(hint);
	WRITE_STRING_FIELD(filename);
	WRITE_INT_FIELD(linenumber);
	WRITE_STRING_FIELD(functionname);
}
