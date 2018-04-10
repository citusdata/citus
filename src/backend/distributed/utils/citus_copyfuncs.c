/*-------------------------------------------------------------------------
 *
 * citus_copyfuncs.c
 *    Citus specific node copy functions
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "distributed/citus_nodefuncs.h"
#include "distributed/multi_server_executor.h"
#include "utils/datum.h"


/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */
static inline Node *
CitusSetTag(Node *node, int tag)
{
	CitusNode *citus_node = (CitusNode *) node;
	citus_node->citus_tag = tag;
	return node;
}


#define DECLARE_FROM_AND_NEW_NODE(nodeTypeName) \
	nodeTypeName * newnode = (nodeTypeName *) \
							 CitusSetTag((Node *) target_node, T_ ## nodeTypeName); \
	nodeTypeName *from = (nodeTypeName *) source_node

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObject(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
	(newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a node array. Target array is also allocated. */
#define COPY_NODE_ARRAY(fldname, type, count) \
	do { \
		int i = 0; \
		newnode->fldname = (type **) palloc(count * sizeof(type *)); \
		for (i = 0; i < count; ++i) \
		{ \
			newnode->fldname[i] = copyObject(from->fldname[i]); \
		} \
	} \
	while (0)

/* Copy a scalar array. Target array is also allocated. */
#define COPY_SCALAR_ARRAY(fldname, type, count) \
	do { \
		int i = 0; \
		newnode->fldname = (type *) palloc(count * sizeof(type)); \
		for (i = 0; i < count; ++i) \
		{ \
			newnode->fldname[i] = from->fldname[i]; \
		} \
	} \
	while (0)


static void
copyJobInfo(Job *newnode, Job *from)
{
	COPY_SCALAR_FIELD(jobId);
	COPY_NODE_FIELD(jobQuery);
	COPY_NODE_FIELD(taskList);
	COPY_NODE_FIELD(dependedJobList);
	COPY_SCALAR_FIELD(subqueryPushdown);
	COPY_SCALAR_FIELD(requiresMasterEvaluation);
	COPY_SCALAR_FIELD(deferredPruning);
}


void
CopyNodeJob(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(Job);

	copyJobInfo(newnode, from);
}


void
CopyNodeDistributedPlan(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(DistributedPlan);

	COPY_SCALAR_FIELD(planId);
	COPY_SCALAR_FIELD(operation);
	COPY_SCALAR_FIELD(hasReturning);

	COPY_NODE_FIELD(workerJob);
	COPY_NODE_FIELD(masterQuery);
	COPY_SCALAR_FIELD(routerExecutable);
	COPY_NODE_FIELD(relationIdList);

	COPY_NODE_FIELD(insertSelectSubquery);
	COPY_NODE_FIELD(insertTargetList);
	COPY_SCALAR_FIELD(targetRelationId);

	COPY_NODE_FIELD(subPlanList);

	COPY_NODE_FIELD(planningError);
}


void
CopyNodeDistributedSubPlan(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(DistributedSubPlan);

	COPY_SCALAR_FIELD(subPlanId);
	COPY_NODE_FIELD(plan);
}


void
CopyNodeShardInterval(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(ShardInterval);

	COPY_SCALAR_FIELD(relationId);
	COPY_SCALAR_FIELD(storageType);
	COPY_SCALAR_FIELD(valueTypeId);
	COPY_SCALAR_FIELD(valueTypeLen);
	COPY_SCALAR_FIELD(valueByVal);
	COPY_SCALAR_FIELD(minValueExists);
	COPY_SCALAR_FIELD(maxValueExists);

	if (from->minValueExists)
	{
		newnode->minValue = datumCopy(from->minValue,
									  from->valueByVal,
									  from->valueTypeLen);
	}
	if (from->maxValueExists)
	{
		newnode->maxValue = datumCopy(from->maxValue,
									  from->valueByVal,
									  from->valueTypeLen);
	}

	COPY_SCALAR_FIELD(shardId);
	COPY_SCALAR_FIELD(shardIndex);
}


void
CopyNodeMapMergeJob(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(MapMergeJob);
	int arrayLength = 0;

	copyJobInfo(&newnode->job, &from->job);

	COPY_NODE_FIELD(reduceQuery);
	COPY_SCALAR_FIELD(partitionType);
	COPY_NODE_FIELD(partitionColumn);
	COPY_SCALAR_FIELD(partitionCount);
	COPY_SCALAR_FIELD(sortedShardIntervalArrayLength);

	arrayLength = from->sortedShardIntervalArrayLength;

	/* now build & read sortedShardIntervalArray */
	COPY_NODE_ARRAY(sortedShardIntervalArray, ShardInterval, arrayLength);

	COPY_NODE_FIELD(mapTaskList);
	COPY_NODE_FIELD(mergeTaskList);
}


void
CopyNodeShardPlacement(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(ShardPlacement);

	COPY_SCALAR_FIELD(placementId);
	COPY_SCALAR_FIELD(shardId);
	COPY_SCALAR_FIELD(shardLength);
	COPY_SCALAR_FIELD(shardState);
	COPY_SCALAR_FIELD(groupId);
	COPY_STRING_FIELD(nodeName);
	COPY_SCALAR_FIELD(nodePort);
	COPY_SCALAR_FIELD(partitionMethod);
	COPY_SCALAR_FIELD(colocationGroupId);
	COPY_SCALAR_FIELD(representativeValue);
}


void
CopyNodeGroupShardPlacement(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(GroupShardPlacement);

	COPY_SCALAR_FIELD(placementId);
	COPY_SCALAR_FIELD(shardId);
	COPY_SCALAR_FIELD(shardLength);
	COPY_SCALAR_FIELD(shardState);
	COPY_SCALAR_FIELD(groupId);
}


void
CopyNodeRelationShard(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(RelationShard);

	COPY_SCALAR_FIELD(relationId);
	COPY_SCALAR_FIELD(shardId);
}


void
CopyNodeTask(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(Task);

	COPY_SCALAR_FIELD(taskType);
	COPY_SCALAR_FIELD(jobId);
	COPY_SCALAR_FIELD(taskId);
	COPY_STRING_FIELD(queryString);
	COPY_SCALAR_FIELD(anchorShardId);
	COPY_NODE_FIELD(taskPlacementList);
	COPY_NODE_FIELD(dependedTaskList);
	COPY_SCALAR_FIELD(partitionId);
	COPY_SCALAR_FIELD(upstreamTaskId);
	COPY_NODE_FIELD(shardInterval);
	COPY_SCALAR_FIELD(assignmentConstrained);
	COPY_NODE_FIELD(taskExecution);
	COPY_SCALAR_FIELD(upsertQuery);
	COPY_SCALAR_FIELD(replicationModel);
	COPY_SCALAR_FIELD(insertSelectQuery);
	COPY_NODE_FIELD(relationShardList);
	COPY_NODE_FIELD(rowValuesLists);
}


void
CopyNodeTaskExecution(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(TaskExecution);

	COPY_SCALAR_FIELD(jobId);
	COPY_SCALAR_FIELD(taskId);
	COPY_SCALAR_FIELD(nodeCount);

	COPY_SCALAR_ARRAY(taskStatusArray, TaskExecStatus, from->nodeCount);
	COPY_SCALAR_ARRAY(transmitStatusArray, TransmitExecStatus, from->nodeCount);
	COPY_SCALAR_ARRAY(connectionIdArray, int32, from->nodeCount);
	COPY_SCALAR_ARRAY(fileDescriptorArray, int32, from->nodeCount);

	COPY_SCALAR_FIELD(connectStartTime);
	COPY_SCALAR_FIELD(currentNodeIndex);
	COPY_SCALAR_FIELD(querySourceNodeIndex);
	COPY_SCALAR_FIELD(failureCount);
}


void
CopyNodeDeferredErrorMessage(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(DeferredErrorMessage);

	COPY_SCALAR_FIELD(code);
	COPY_STRING_FIELD(message);
	COPY_STRING_FIELD(detail);
	COPY_STRING_FIELD(hint);
	COPY_STRING_FIELD(filename);
	COPY_SCALAR_FIELD(linenumber);
	COPY_STRING_FIELD(functionname);
}
