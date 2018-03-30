/*-------------------------------------------------------------------------
 *
 * multi_physical_planner.h
 *	  Type and function declarations used in creating the distributed execution
 *	  plan.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PHYSICAL_PLANNER_H
#define MULTI_PHYSICAL_PLANNER_H

#include "postgres.h"
#include "c.h"

#include "datatype/timestamp.h"
#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/distributed_planner.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/array.h"


/* Definitions local to the physical planner */
#define ARRAY_OUT_FUNC_ID 751
#define NON_PRUNABLE_JOIN -1
#define RESERVED_HASHED_COLUMN_ID MaxAttrNumber
#define MERGE_COLUMN_FORMAT "merge_column_%u"
#define MAP_OUTPUT_FETCH_COMMAND "SELECT worker_fetch_partition_file \
 (" UINT64_FORMAT ", %u, %u, %u, '%s', %u)"
#define RANGE_PARTITION_COMMAND "SELECT worker_range_partition_table \
 (" UINT64_FORMAT ", %d, %s, '%s', '%s'::regtype, %s)"
#define HASH_PARTITION_COMMAND "SELECT worker_hash_partition_table \
 (" UINT64_FORMAT ", %d, %s, '%s', '%s'::regtype, %d)"
#define MERGE_FILES_INTO_TABLE_COMMAND "SELECT worker_merge_files_into_table \
 (" UINT64_FORMAT ", %d, '%s', '%s')"
#define MERGE_FILES_AND_RUN_QUERY_COMMAND \
	"SELECT worker_merge_files_and_run_query(" UINT64_FORMAT ", %d, %s, %s)"


typedef enum CitusRTEKind
{
	CITUS_RTE_RELATION = RTE_RELATION,  /* ordinary relation reference */
	CITUS_RTE_SUBQUERY = RTE_SUBQUERY,  /* subquery in FROM */
	CITUS_RTE_JOIN = RTE_JOIN,          /* join */
	CITUS_RTE_FUNCTION = RTE_FUNCTION,  /* function in FROM */
#if (PG_VERSION_NUM >= 100000)
	CITUS_RTE_TABLEFUNC = RTE_TABLEFUNC, /* TableFunc(.., column list) */
#endif
	CITUS_RTE_VALUES = RTE_VALUES,      /* VALUES (<exprlist>), (<exprlist>), ... */
	CITUS_RTE_CTE = RTE_CTE,            /* common table expr (WITH list element) */
#if (PG_VERSION_NUM >= 100000)
	CITUS_RTE_NAMEDTUPLESTORE = RTE_NAMEDTUPLESTORE, /* tuplestore, e.g. for triggers */
#endif
	CITUS_RTE_SHARD,
	CITUS_RTE_REMOTE_QUERY
} CitusRTEKind;


/* Enumeration that defines the partition type for a remote job */
typedef enum
{
	PARTITION_INVALID_FIRST = 0,
	RANGE_PARTITION_TYPE = 1,
	HASH_PARTITION_TYPE = 2
} PartitionType;


/* Enumeration that defines different task types */
typedef enum
{
	TASK_TYPE_INVALID_FIRST = 0,
	SQL_TASK = 1,
	MAP_TASK = 2,
	MERGE_TASK = 3,
	MAP_OUTPUT_FETCH_TASK = 4,
	MERGE_FETCH_TASK = 5,
	MODIFY_TASK = 6,
	ROUTER_TASK = 7,
	DDL_TASK = 8
} TaskType;


/* Enumeration that defines the task assignment policy to use */
typedef enum
{
	TASK_ASSIGNMENT_INVALID_FIRST = 0,
	TASK_ASSIGNMENT_GREEDY = 1,
	TASK_ASSIGNMENT_ROUND_ROBIN = 2,
	TASK_ASSIGNMENT_FIRST_REPLICA = 3
} TaskAssignmentPolicyType;


/* Enumeration that defines different job types */
typedef enum
{
	JOB_INVALID_FIRST = 0,
	JOIN_MAP_MERGE_JOB = 1,
	SUBQUERY_MAP_MERGE_JOB = 2,
	TOP_LEVEL_WORKER_JOB = 3
} BoundaryNodeJobType;


/*
 * Job represents a logical unit of work that contains one set of data transfers
 * in our physical plan. The physical planner maps each SQL query into one or
 * more jobs depending on the query's complexity, and sets dependencies between
 * these jobs. Each job consists of multiple executable tasks; and these tasks
 * either operate on base shards, or repartitioned tables.
 */
typedef struct Job
{
	CitusNode type;
	uint64 jobId;
	Query *jobQuery;
	List *taskList;
	List *dependedJobList;
	bool subqueryPushdown;
	bool requiresMasterEvaluation; /* only applies to modify jobs */
	bool deferredPruning;
} Job;


/* Defines a repartitioning job and holds additional related data. */
typedef struct MapMergeJob
{
	Job job;
	Query *reduceQuery;
	PartitionType partitionType;
	Var *partitionColumn;
	uint32 partitionCount;
	int sortedShardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray; /* only applies to range partitioning */
	List *mapTaskList;
	List *mergeTaskList;
} MapMergeJob;


/*
 * Task represents an executable unit of work. We conceptualize our tasks into
 * compute and data fetch task types. SQL, map, and merge tasks are considered
 * as compute tasks; and map fetch, and merge fetch tasks are data
 * fetch tasks. We also forward declare the task execution struct here to avoid
 * including the executor header files.
 *
 * We currently do not take replication model into account for tasks other
 * than modifications. When it is set to REPLICATION_MODEL_2PC, the execution
 * of the modification task is done with two-phase commit. Set it to
 * REPLICATION_MODEL_INVALID if it is not relevant for the task.
 *
 * NB: Changing this requires also changing _outTask in citus_outfuncs and _readTask
 * in citus_readfuncs to correctly (de)serialize this struct.
 */
typedef struct TaskExecution TaskExecution;

typedef struct Task
{
	CitusNode type;
	TaskType taskType;
	uint64 jobId;
	uint32 taskId;
	char *queryString;
	uint64 anchorShardId;       /* only applies to compute tasks */
	List *taskPlacementList;    /* only applies to compute tasks */
	List *dependedTaskList;     /* only applies to compute tasks */

	uint32 partitionId;
	uint32 upstreamTaskId;         /* only applies to data fetch tasks */
	ShardInterval *shardInterval;  /* only applies to merge tasks */
	bool assignmentConstrained;    /* only applies to merge tasks */
	TaskExecution *taskExecution;  /* used by task tracker executor */
	bool upsertQuery;              /* only applies to modify tasks */
	char replicationModel;         /* only applies to modify tasks */

	bool insertSelectQuery;
	List *relationShardList;

	List *rowValuesLists;          /* rows to use when building multi-row INSERT */
} Task;


/*
 * RangeTableFragment represents a fragment of a range table. This fragment
 * could be a regular shard or a merged table formed in a MapMerge job.
 */
typedef struct RangeTableFragment
{
	CitusRTEKind fragmentType;
	void *fragmentReference;
	uint32 rangeTableId;
} RangeTableFragment;


/*
 * JoinSequenceNode represents a range table in an ordered sequence of tables
 * joined together. This representation helps build combinations of all range
 * table fragments during task generation.
 */
typedef struct JoinSequenceNode
{
	uint32 rangeTableId;
	int32 joiningRangeTableId;
} JoinSequenceNode;


/*
 * DistributedPlan contains all information necessary to execute a
 * distribute query.
 */
typedef struct DistributedPlan
{
	CitusNode type;

	/* unique identifier of the plan within the session */
	uint64 planId;

	/* type of command to execute (SELECT/INSERT/...) */
	CmdType operation;

	/* specifies whether a DML command has a RETURNING */
	bool hasReturning;

	/* job tree containing the tasks to be executed on workers */
	Job *workerJob;

	/* local query that merges results from the workers */
	Query *masterQuery;

	/* a router executable query is executed entirely on a worker */
	bool routerExecutable;

	/* which relations are accessed by this distributed plan */
	List *relationIdList;

	/* SELECT query in an INSERT ... SELECT via the coordinator */
	Query *insertSelectSubquery;

	/* target list of an INSERT ... SELECT via the coordinator */
	List *insertTargetList;

	/* target relation of an INSERT ... SELECT via the coordinator */
	Oid targetRelationId;

	/* list of subplans to execute before the distributed query */
	List *subPlanList;

	/*
	 * NULL if this a valid plan, an error description otherwise. This will
	 * e.g. be set if SQL features are present that a planner doesn't support,
	 * or if prepared statement parameters prevented successful planning.
	 */
	DeferredErrorMessage *planningError;
} DistributedPlan;


/*
 * DistributedSubPlan contains a subplan of a distributed plan. Subplans are
 * executed before the distributed query and their results are written to
 * temporary files. This is used to execute CTEs and subquery joins that
 * cannot be distributed.
 */
typedef struct DistributedSubPlan
{
	CitusNode type;

	uint32 subPlanId;
	PlannedStmt *plan;
} DistributedSubPlan;


/* OperatorCacheEntry contains information for each element in OperatorCache */
typedef struct OperatorCacheEntry
{
	/* cache key consists of typeId, accessMethodId and strategyNumber */
	Oid typeId;
	Oid accessMethodId;
	int16 strategyNumber;
	Oid operatorId;
	Oid operatorClassInputType;
	char typeType;
} OperatorCacheEntry;


/* Config variable managed via guc.c */
extern int TaskAssignmentPolicy;
extern bool EnableUniqueJobIds;


/* Function declarations for building physical plans and constructing queries */
extern DistributedPlan * CreatePhysicalDistributedPlan(MultiTreeRoot *multiTree,
													   PlannerRestrictionContext *
													   plannerRestrictionContext);
extern Task * CreateBasicTask(uint64 jobId, uint32 taskId, TaskType taskType,
							  char *queryString);

extern OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);

/*
 * Function declarations for building, updating constraints and simple operator
 * expression check.
 */
extern Node * BuildBaseConstraint(Var *column);
extern void UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval);
extern bool SimpleOpExpression(Expr *clause);
extern bool OpExpressionContainsColumn(OpExpr *operatorExpression, Var *partitionColumn);

/* helper functions */
extern Var * MakeInt4Column(void);
extern Const * MakeInt4Constant(Datum constantValue);
extern int CompareShardPlacements(const void *leftElement, const void *rightElement);
extern bool ShardIntervalsOverlap(ShardInterval *firstInterval,
								  ShardInterval *secondInterval);

/* function declarations for Task and Task list operations */
extern bool TasksEqual(const Task *a, const Task *b);
extern List * TaskListAppendUnique(List *list, Task *task);
extern List * TaskListConcatUnique(List *list1, List *list2);
extern bool TaskListMember(const List *taskList, const Task *task);
extern List * TaskListDifference(const List *list1, const List *list2);
extern List * AssignAnchorShardTaskList(List *taskList);
extern List * FirstReplicaAssignTaskList(List *taskList);


#endif   /* MULTI_PHYSICAL_PLANNER_H */
