//
// Created by Nils Dijk on 17/01/2020.
//
#include "postgres.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/path_based_planner.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/restrictinfo.h"

static Plan * CreateDistributedUnionPlan(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *best_path, List *tlist, List *clauses, List *custom_plans);
static List * ReparameterizeDistributedUnion(PlannerInfo *root, List *custom_private, RelOptInfo *child_rel);
static CustomPath * WrapTableAccessWithDistributedUnion(PlannerInfo *root, Path *originalPath);
static Index VarnoFromFirstTargetEntry(List *tlist);
static Query * GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses);
static List * ShardIntervalListToRelationShardList(List *shardIntervalList);

static bool IsDistributedUnion(Path *path);

typedef struct DistributedUnionPath
{
	CustomPath custom_path;

	/* path to be executed on the worker */
	Path *worker_path;

	uint32 colocationId;
} DistributedUnionPath;

const CustomPathMethods distributedUnionMethods = {
	.CustomName = "Distributed Union",
	.PlanCustomPath = CreateDistributedUnionPlan,
	.ReparameterizeCustomPathByChild = ReparameterizeDistributedUnion
};


CustomPath *
WrapTableAccessWithDistributedUnion(PlannerInfo *root, Path *originalPath)
{
	DistributedUnionPath *distUnion = newNode(sizeof(DistributedUnionPath), T_CustomPath);
	distUnion->custom_path.path.pathtype = T_CustomScan;
	distUnion->custom_path.path.parent = originalPath->parent;
	distUnion->custom_path.path.pathtarget = originalPath->pathtarget;
	distUnion->custom_path.path.param_info = originalPath->param_info;

	/* TODO use a better cost model */
	distUnion->custom_path.path.rows = originalPath->rows;
	distUnion->custom_path.path.startup_cost = originalPath->startup_cost+1000;
	distUnion->custom_path.path.total_cost = originalPath->total_cost+1000;

	distUnion->custom_path.methods = &distributedUnionMethods;

	distUnion->worker_path = originalPath;

	/* collect the colocation group of the table */
	RangeTblEntry *rte = root->simple_rte_array[originalPath->parent->relid];
	distUnion->colocationId = TableColocationId(rte->relid);

	return (CustomPath *) distUnion;
}


static Plan *
CreateDistributedUnionPlan(PlannerInfo *root,
						   RelOptInfo *rel,
						   struct CustomPath *best_path,
						   List *tlist,
						   List *clauses,
						   List *custom_plans)
{
	DistributedUnionPath *distUnion = (DistributedUnionPath *) best_path;

	Job *workerJob = CitusMakeNode(Job);
	workerJob->jobId = UniqueJobId();

	ShardInterval *shardInterval = NULL;

	Query *q = GetQueryFromPath(root, distUnion->worker_path, tlist, clauses);
	/*
	 * Assume shards are colocated, any shard should suffice for now to find the initial
	 * interval list
	 */
	/* TODO track colocation information on the Distributed Union node to fetch required information in a more optimal setting*/
	RangeTblEntry *rte = linitial_node(RangeTblEntry, q->rtable);
	List* shardIntervalList = LoadShardIntervalList(rte->relid);

	int i = 0;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		List *colocatedShards = ColocatedShardIntervalList(shardInterval);
		List *relationShardList = ShardIntervalListToRelationShardList(colocatedShards);

		Query *qc = copyObject(q);
		UpdateRelationToShardNames((Node *) qc, relationShardList);

		StringInfoData buf;
		initStringInfo(&buf);
		pg_get_query_def(qc, &buf);

		Task *sqlTask = CreateBasicTask(workerJob->jobId, i, SELECT_TASK, buf.data);
		sqlTask->anchorShardId = shardInterval->shardId;
		sqlTask->taskPlacementList = FinalizedShardPlacementList(shardInterval->shardId);
		workerJob->taskList = lappend(workerJob->taskList, sqlTask);
		i++;
	}

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = workerJob;
	distributedPlan->modLevel = ROW_MODIFY_READONLY;
	distributedPlan->relationIdList = list_make1_oid(rte->relid);
	distributedPlan->hasReturning = true;

	CustomScan *plan = makeNode(CustomScan);
	plan->scan.scanrelid = VarnoFromFirstTargetEntry(tlist);
	plan->flags = best_path->flags;
	plan->methods = &AdaptiveExecutorCustomScanMethods;
	plan->custom_private = list_make1(distributedPlan);

	plan->scan.plan.targetlist = tlist;
	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

	plan->scan.plan.qual = clauses;
	plan->custom_exprs = clauses;

	return (Plan *) plan;
}


static List *
ShardIntervalListToRelationShardList(List *shardIntervalList)
{
	List *shardRelationList = NIL;
	ShardInterval *shardInterval = NULL;

	/* map the shard intervals to RelationShard */
	foreach_ptr(shardInterval, shardIntervalList)
	{
		RelationShard *rs = CitusMakeNode(RelationShard);
		rs->relationId = shardInterval->relationId;
		rs->shardId = shardInterval->shardId;
		shardRelationList = lappend(shardRelationList, rs);
	}

	return shardRelationList;
}


static List *
ReparameterizeDistributedUnion(PlannerInfo *root,
							   List *custom_private,
							   RelOptInfo *child_rel)
{
	return NIL;
}


/*
 * IsDistributedUnion returns if the pathnode is a distributed union
 */
static bool
IsDistributedUnion(Path *path)
{
	if (!IsA(path, CustomPath))
	{
		return false;
	}

	CustomPath *cpath = castNode(CustomPath, path);
	return cpath->methods == &distributedUnionMethods;
}


void
PathBasedPlannerRelationHook(PlannerInfo *root, RelOptInfo *relOptInfo, Index restrictionIndex, RangeTblEntry *rte)
{
	if (!IsDistributedTable(rte->relid))
	{
		/* table accessed is not distributed, no paths to change */
		return;
	}

	/* wrap every path with a distirbuted union custom path */
	ListCell *pathCell = NULL;
	foreach(pathCell, relOptInfo->pathlist)
	{
		Path *originalPath = lfirst(pathCell);
		pathCell->data.ptr_value = WrapTableAccessWithDistributedUnion(root, originalPath);
	}
}



/*
 * varno_mapping is an array where the index is the varno in the original query, or 0 if
 * no mapping is required.
 */
static Node *
VarNoMutator(Node *expr, Index *varno_mapping)
{
	switch (nodeTag(expr))
	{
		case T_Var:
		{
			Var *var = castNode(Var, expr);
			Index newVarNo = varno_mapping[var->varno];
			if (newVarNo == 0)
			{
				/* no mapping required */
				return (Node *) var;
			}

			return (Node *) makeVar(
				newVarNo,
				var->varattno,
				var->vartype,
				var->vartypmod,
				var->varcollid,
				var->varlevelsup
			);
		}

		default:
		{
			return expression_tree_mutator(expr, (void*) VarNoMutator, varno_mapping);
		}
	}
}


static Query *
GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses)
{
	Index scan_relid = path->parent->relid;
	RangeTblEntry *rte = root->simple_rte_array[scan_relid];

	Query *q = makeNode(Query);
	q->commandType = CMD_SELECT;
	q->rtable = list_make1(rte);

	List *newTargetList = NIL;
	TargetEntry *target = NULL;

	Index *varno_mapping = palloc0(sizeof(Index) * root->simple_rel_array_size);
	/*
	 * map the rte index of the table we are scanning to the range table entry as we have
	 * added it to the query
	 */
	varno_mapping[scan_relid] = 1;

	/* copy the target list with mapped varno values to reflect the tables we are selecting */
	newTargetList = (List *) VarNoMutator((Node *) tlist, varno_mapping);

	q->targetList = newTargetList;
	q->jointree = makeNode(FromExpr);
	RangeTblRef *rr = makeNode(RangeTblRef);
	rr->rtindex = 1;
	q->jointree->fromlist = list_make1(rr);

	List *quals = NIL;

	RestrictInfo *rinfo = NULL;
	foreach_ptr(rinfo, clauses)
	{
		Node *clause = (Node *) rinfo->clause;
		clause = VarNoMutator(clause, varno_mapping);
		quals = lappend(quals, clause);
	}
	q->jointree->quals = (Node *) quals;

	return q;
}


static Index
VarnoFromFirstTargetEntry(List *tlist)
{
	TargetEntry *entry = linitial_node(TargetEntry, tlist);
	Var *var = castNode(Var, entry->expr);
	return var->varno;
}
