/*-------------------------------------------------------------------------
 *
 * multi_physical_planner.h
 *	  Type and function declarations used in creating the distributed execution
 *	  plan.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/array.h"

#include "pg_version_constants.h"

#include "distributed/citus_nodes.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/worker_manager.h"


/* Definitions local to the physical planner */
#define NON_PRUNABLE_JOIN -1
#define RESERVED_HASHED_COLUMN_ID MaxAttrNumber

extern int RepartitionJoinBucketCountPerNode;

typedef enum CitusRTEKind
{
	CITUS_RTE_RELATION = RTE_RELATION,  /* ordinary relation reference */
	CITUS_RTE_SUBQUERY = RTE_SUBQUERY,  /* subquery in FROM */
	CITUS_RTE_JOIN = RTE_JOIN,          /* join */
	CITUS_RTE_FUNCTION = RTE_FUNCTION,  /* function in FROM */
	CITUS_RTE_TABLEFUNC = RTE_TABLEFUNC, /* TableFunc(.., column list) */
	CITUS_RTE_VALUES = RTE_VALUES,      /* VALUES (<exprlist>), (<exprlist>), ... */
	CITUS_RTE_CTE = RTE_CTE,            /* common table expr (WITH list element) */
	CITUS_RTE_NAMEDTUPLESTORE = RTE_NAMEDTUPLESTORE, /* tuplestore, e.g. for triggers */
	CITUS_RTE_RESULT = RTE_RESULT,      /* RTE represents an empty FROM clause */
	CITUS_RTE_SHARD,
	CITUS_RTE_REMOTE_QUERY
} CitusRTEKind;


/* Enumeration that defines the partition type for a remote job */
typedef enum
{
	PARTITION_INVALID_FIRST = 0,
	RANGE_PARTITION_TYPE = 1,
	SINGLE_HASH_PARTITION_TYPE = 2,
	DUAL_HASH_PARTITION_TYPE = 3
} PartitionType;


/* Enumeration that defines different task types */
typedef enum
{
	TASK_TYPE_INVALID_FIRST,
	READ_TASK,
	MAP_TASK,
	MERGE_TASK,
	MAP_OUTPUT_FETCH_TASK,
	MERGE_FETCH_TASK,
	MODIFY_TASK,
	DDL_TASK,
	VACUUM_ANALYZE_TASK
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
	TOP_LEVEL_WORKER_JOB = 2
} BoundaryNodeJobType;


/* Enumeration that specifies extent of DML modifications */
typedef enum RowModifyLevel
{
	ROW_MODIFY_NONE = 0,
	ROW_MODIFY_READONLY = 1,
	ROW_MODIFY_COMMUTATIVE = 2,
	ROW_MODIFY_NONCOMMUTATIVE = 3
} RowModifyLevel;


/*
 * LocalPlannedStatement represents a local plan of a shard. The scope
 * for the LocalPlannedStatement is Task.
 */
typedef struct LocalPlannedStatement
{
	CitusNode type;

	uint64 shardId;
	uint32 localGroupId;
	PlannedStmt *localPlan;
} LocalPlannedStatement;


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
	List *dependentJobList;
	bool subqueryPushdown;
	bool requiresCoordinatorEvaluation; /* only applies to modify jobs */
	bool deferredPruning;
	Const *partitionKeyValue;

	/* for local shard queries, we may save the local plan here */
	List *localPlannedStatements;

	/*
	 * When we evaluate functions and parameters in jobQuery then we
	 * should no longer send the list of parameters along with the
	 * query.
	 */
	bool parametersInJobQueryResolved;
	uint32 colocationId; /* common colocation group ID of the relations */
} Job;


/* Defines a repartitioning job and holds additional related data. */
typedef struct MapMergeJob
{
	Job job;
	PartitionType partitionType;
	Var *partitionColumn;
	uint32 partitionCount;
	int sortedShardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray; /* only applies to range partitioning */
	List *mapTaskList;
	List *mergeTaskList;
} MapMergeJob;

typedef enum TaskQueryType
{
	TASK_QUERY_NULL,
	TASK_QUERY_TEXT,
	TASK_QUERY_OBJECT,
	TASK_QUERY_TEXT_LIST
} TaskQueryType;

typedef struct TaskQuery
{
	TaskQueryType queryType;

	union
	{
		/*
		 * For most queries jobQueryReferenceForLazyDeparsing and/or queryStringLazy is not
		 * NULL. This means we have a single query for all placements.
		 *
		 * If this is not the case, the length of perPlacementQueryStrings is
		 * non-zero and equal to length of taskPlacementList. Like this it can
		 * assign a different query for each placement. We need this flexibility
		 * when a query should return node specific values. For example, on which
		 * node did we succeed storing some result files?
		 *
		 * jobQueryReferenceForLazyDeparsing is only not null when the planner thinks the
		 * query could possibly be locally executed. In that case deparsing+parsing
		 * the query might not be necessary, so we do that lazily.
		 *
		 * jobQueryReferenceForLazyDeparsing should only be set by using SetTaskQueryIfShouldLazyDeparse()
		 */
		Query *jobQueryReferenceForLazyDeparsing;

		/*
		 * In almost all cases queryStringLazy should be read only indirectly by
		 * using TaskQueryString(). This will populate the field if only the
		 * jobQueryReferenceForLazyDeparsing field is not NULL.
		 *
		 * This field should only be set by using SetTaskQueryString() (or as a
		 * side effect from TaskQueryString()). Otherwise it might not be in sync
		 * with jobQueryReferenceForLazyDeparsing.
		 */
		char *queryStringLazy;

		/*
		 * queryStringList contains query strings. They should be
		 * run sequentially. The concatenated version of this list
		 * will already be set for queryStringLazy, this can be useful
		 * when we want to access each query string.
		 */
		List *queryStringList;
	}data;
}TaskQuery;

struct TupleDestination;

typedef struct Task
{
	CitusNode type;
	TaskType taskType;
	uint64 jobId;
	uint32 taskId;

	/*
	 * taskQuery contains query string information. The way we get queryString can be different
	 * so this is abstracted with taskQuery.
	 */
	TaskQuery taskQuery;

	/*
	 * A task can have multiple queries, in which case queryCount will be > 1, and
	 * taskQuery->queryType == TASK_QUERY_TEXT_LIST.
	 */
	int queryCount;

	Oid anchorDistributedTableId;     /* only applies to insert tasks */
	uint64 anchorShardId;       /* only applies to compute tasks */
	List *taskPlacementList;    /* only applies to compute tasks */
	List *dependentTaskList;     /* only applies to compute tasks */

	uint32 partitionId;
	uint32 upstreamTaskId;         /* only applies to data fetch tasks */
	ShardInterval *shardInterval;  /* only applies to merge tasks */
	bool assignmentConstrained;    /* only applies to merge tasks */

	/* for merge tasks, this is set to the target list of the map task */
	List *mapJobTargetList;

	char replicationModel;         /* only applies to modify tasks */

	/*
	 * List of struct RelationRowLock. This contains an entry for each
	 * query identified as a FOR [KEY] UPDATE/SHARE target. Citus
	 * converts PostgreSQL's RowMarkClause to RelationRowLock in
	 * RowLocksOnRelations().
	 */
	List *relationRowLockList;

	bool modifyWithSubquery;

	/*
	 * List of struct RelationShard. This represents the mapping of relations
	 * in the RTE list to shard IDs for a task for the purposes of:
	 *  - Locking: See AcquireExecutorShardLocks()
	 *  - Deparsing: See UpdateRelationToShardNames()
	 *  - Relation Access Tracking
	 */
	List *relationShardList;

	List *rowValuesLists;          /* rows to use when building multi-row INSERT */

	/*
	 * Used only when local execution happens. Indicates that this task is part of
	 * both local and remote executions. We use "or" in the field name because this
	 * is set to true for both the remote and local tasks generated for such
	 * executions. The most common example is modifications to reference tables where
	 * the task splitted into local and remote tasks.
	 */
	bool partiallyLocalOrRemote;

	/*
	 * When we evaluate functions and parameters in the query string then
	 * we should no longer send the list of parameters along with the
	 * query.
	 */
	bool parametersInQueryStringResolved;

	/*
	 * Destination of tuples generated as a result of executing this task. Can be
	 * NULL, in which case executor might use a default destination.
	 */
	struct TupleDestination *tupleDest;

	/*
	 * totalReceivedTupleData only counts the data for a single placement. So
	 * for RETURNING DML this is not really correct. This is used by
	 * EXPLAIN ANALYZE, to display the amount of received bytes. The local execution
	 * does not increment this value, so only used for remote execution.
	 */
	uint64 totalReceivedTupleData;

	/*
	 * EXPLAIN ANALYZE output fetched from worker. This is saved to be used later
	 * by RemoteExplain().
	 */
	char *fetchedExplainAnalyzePlan;
	int fetchedExplainAnalyzePlacementIndex;

	/*
	 * Execution Duration fetched from worker. This is saved to be used later by
	 * ExplainTaskList().
	 */
	double fetchedExplainAnalyzeExecutionDuration;

	/*
	 * isLocalTableModification is true if the task is on modifying a local table.
	 */
	bool isLocalTableModification;

	/*
	 * Vacuum, create/drop/reindex concurrently cannot be executed in a transaction.
	 */
	bool cannotBeExecutedInTransaction;

	Const *partitionKeyValue;
	int colocationId;
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
 * ModifyWithSelectMethod represents the method to use for INSERT INTO ... SELECT
 * or MERGE type of queries.
 *
 * Note that there is a third method which is not represented here, which is
 * pushing down the MERGE/INSERT INTO ... SELECT to workers. This method is
 * executed similar to other distributed queries and doesn't need a special
 * execution code, so we don't need to represent it here.
 */
typedef enum ModifyWithSelectMethod
{
	MODIFY_WITH_SELECT_VIA_COORDINATOR,
	MODIFY_WITH_SELECT_REPARTITION
} ModifyWithSelectMethod;


/*
 * DistributedPlan contains all information necessary to execute a
 * distribute query.
 */
typedef struct DistributedPlan
{
	CitusNode type;

	/* unique identifier of the plan within the session */
	uint64 planId;

	/* specifies nature of modifications in query */
	RowModifyLevel modLevel;

	/*
	 * specifies whether plan returns results,
	 * either as a SELECT or a DML which has RETURNING.
	 */
	bool expectResults;

	/* job tree containing the tasks to be executed on workers */
	Job *workerJob;

	/* local query that merges results from the workers */
	Query *combineQuery;

	/* query identifier (copied from the top-level PlannedStmt) */
	uint64 queryId;

	/* which relations are accessed by this distributed plan */
	List *relationIdList;

	/* target relation of a modification */
	Oid targetRelationId;

	/*
	 * Modifications performed using the output of a source query via
	 * the coordinator or repartition.
	 */
	Query *modifyQueryViaCoordinatorOrRepartition;
	PlannedStmt *selectPlanForModifyViaCoordinatorOrRepartition;
	ModifyWithSelectMethod modifyWithSelectMethod;

	/*
	 * If intermediateResultIdPrefix is non-null, the source query
	 * results are written to a set of intermediate results named
	 * according to <intermediateResultIdPrefix>_<anchorShardId>.
	 * That way we can run a distributed modification query which
	 * requires evaluating source query results at the coordinator.
	 * Once results are captured in intermediate files, modification
	 * is done from the intermediate results into the target relation.
	 *
	 */
	char *intermediateResultIdPrefix;

	/* list of subplans to execute before the distributed query */
	List *subPlanList;

	/*
	 * List of subPlans that are used in the DistributedPlan
	 * Note that this is different that "subPlanList" field which
	 * contains the subplans generated as part of the DistributedPlan.
	 *
	 * On the other hand, usedSubPlanNodeList keeps track of which subPlans
	 * are used within this distributed plan as a list of
	 * UsedDistributedSubPlan pointers.
	 *
	 * The list may contain duplicates if the subplan is referenced multiple
	 * times (e.g. a CTE appears in the query tree multiple times).
	 */
	List *usedSubPlanNodeList;

	/*
	 * When the query is very simple such that we don't need to call
	 * standard_planner(). See FastPathRouterQuery() for the definition.
	 */
	bool fastPathRouterPlan;

	/* number of times this plan has been used (as a prepared statement) */
	uint32 numberOfTimesExecuted;

	/*
	 * NULL if this a valid plan, an error description otherwise. This will
	 * e.g. be set if SQL features are present that a planner doesn't support,
	 * or if prepared statement parameters prevented successful planning.
	 */
	DeferredErrorMessage *planningError;

	/*
	 * When performing query execution scenarios that require repartitioning
	 * the source rows, this field stores the index of the column in the list
	 * of source rows to be repartitioned for colocation with the target.
	 */
	int sourceResultRepartitionColumnIndex;
} DistributedPlan;


/*
 *
 */
typedef struct SubPlanExplainOutput
{
	char *explainOutput;
	double executionDuration;
	uint64 totalReceivedTupleData;
} SubPlanExplainOutput;


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

	/* EXPLAIN ANALYZE instrumentations */
	uint64 bytesSentPerWorker;
	uint32 remoteWorkerCount;
	double durationMillisecs;
	bool writeLocalFile;
	SubPlanExplainOutput totalExplainOutput[MAX_ANALYZE_OUTPUT];
	uint32 numTasksOutput;
} DistributedSubPlan;


/* defines how a subplan is used by a distributed query */
typedef enum SubPlanAccessType
{
	SUBPLAN_ACCESS_NONE,
	SUBPLAN_ACCESS_LOCAL,
	SUBPLAN_ACCESS_REMOTE,
	SUBPLAN_ACCESS_ANYWHERE
} SubPlanAccessType;


/*
 * UsedDistributedSubPlan contains information about a subPlan that is used in a
 * distributed plan.
 */
typedef struct UsedDistributedSubPlan
{
	CitusNode type;

	/* subplan used by the distributed query */
	char *subPlanId;

	/* how the subplan is used by a distributed query */
	SubPlanAccessType accessType;
} UsedDistributedSubPlan;


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


/* Named function pointer type for reordering Task lists */
typedef List *(*ReorderFunction)(List *);


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
extern Node *  WrapUngroupedVarsInAnyValueAggregate(Node *expression,
													List *groupClauseList,
													List *targetList,
													bool checkExpressionEquality);

/*
 * Function declarations for building, updating constraints and simple operator
 * expression check.
 */
extern Node * BuildBaseConstraint(Var *column);
extern void UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval);
extern bool BinaryOpExpression(Expr *clause, Node **leftOperand, Node **rightOperand);

/* helper functions */
extern Var * MakeInt4Column(void);
extern int CompareShardPlacements(const void *leftElement, const void *rightElement);
extern int CompareGroupShardPlacements(const void *leftElement, const void *rightElement);
extern bool ShardIntervalsOverlap(ShardInterval *firstInterval,
								  ShardInterval *secondInterval);
extern bool ShardIntervalsOverlapWithParams(Datum firstMin, Datum firstMax,
											Datum secondMin, Datum secondMax,
											FmgrInfo *comparisonFunction,
											Oid collation);
extern bool CoPartitionedTables(Oid firstRelationId, Oid secondRelationId);
extern ShardInterval ** GenerateSyntheticShardIntervalArray(int partitionCount);
extern RowModifyLevel RowModifyLevelForQuery(Query *query);
extern StringInfo ArrayObjectToString(ArrayType *arrayObject,
									  Oid columnType, int32 columnTypeMod);


/* function declarations for Task and Task list operations */
extern bool TasksEqual(const Task *a, const Task *b);
extern bool TaskListMember(const List *taskList, const Task *task);
extern List * TaskListDifference(const List *list1, const List *list2);
extern List * AssignAnchorShardTaskList(List *taskList);
extern List * FirstReplicaAssignTaskList(List *taskList);
extern List * RoundRobinAssignTaskList(List *taskList);
extern List * RoundRobinReorder(List *placementList);
extern void SetPlacementNodeMetadata(ShardPlacement *placement, WorkerNode *workerNode);
extern int CompareTasksByTaskId(const void *leftElement, const void *rightElement);
extern int CompareTasksByExecutionDuration(const void *leftElement, const
										   void *rightElement);

/* function declaration for creating Task */
extern List * QueryPushdownSqlTaskList(Query *query, uint64 jobId,
									   RelationRestrictionContext *
									   relationRestrictionContext,
									   List *prunedRelationShardList, TaskType taskType,
									   bool modifyRequiresCoordinatorEvaluation,
									   DeferredErrorMessage **planningError);

extern bool ModifyLocalTableJob(Job *job);

/* function declarations for managing jobs */
extern uint64 UniqueJobId(void);


extern List * DerivedColumnNameList(uint32 columnCount, uint64 generatingJobId);
extern RangeTblEntry * DerivedRangeTableEntry(MultiNode *multiNode, List *columnList,
											  List *tableIdList,
											  List *funcColumnNames,
											  List *funcColumnTypes,
											  List *funcColumnTypeMods,
											  List *funcCollations);

extern List * FetchEqualityAttrNumsForRTE(Node *quals);

#endif   /* MULTI_PHYSICAL_PLANNER_H */
