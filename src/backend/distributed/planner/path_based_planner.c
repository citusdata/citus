//
// Created by Nils Dijk on 17/01/2020.
//
#include "postgres.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
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
static CustomPath * WrapTableAccessWithDistributedUnion(Path *originalPath, RangeTblEntry *rte);
static Index VarnoFromFirstTargetEntry(List *tlist);
static Query * GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses);

static bool IsDistributedUnion(Path *path);
static uint32 ColocationGroupForDistributedUnion(Path *path);

const CustomPathMethods distributedUnionMethods = {
	.CustomName = "Distributed Union",
	.PlanCustomPath = CreateDistributedUnionPlan,
	.ReparameterizeCustomPathByChild = ReparameterizeDistributedUnion
};


CustomPath *
WrapTableAccessWithDistributedUnion(Path *originalPath, RangeTblEntry *rte)
{
	CustomPath *distUnion = makeNode(CustomPath);
	distUnion->path.pathtype = T_CustomScan;
	distUnion->path.parent = originalPath->parent;
	distUnion->path.pathtarget = originalPath->pathtarget;
	distUnion->path.param_info = originalPath->param_info;

	/* TODO use a better cost model */
	distUnion->path.rows = originalPath->rows;
	distUnion->path.startup_cost = originalPath->startup_cost+1000;
	distUnion->path.total_cost = originalPath->total_cost+1000;

	distUnion->methods = &distributedUnionMethods;

	distUnion->custom_private = list_make2(rte, originalPath);

	return distUnion;
}


static Plan *
CreateDistributedUnionPlan(PlannerInfo *root,
						   RelOptInfo *rel,
						   struct CustomPath *best_path,
						   List *tlist,
						   List *clauses,
						   List *custom_plans)
{
	Job *workerJob = CitusMakeNode(Job);
	workerJob->jobId = UniqueJobId();

	RangeTblEntry *rte = list_nth_node(RangeTblEntry, best_path->custom_private, 0);
	Path *originalPath = (Path *) list_nth(best_path->custom_private, 1);
	List* shardIntervalList = LoadShardIntervalList(rte->relid);
	ShardInterval *shardInterval = NULL;

	Query *q = GetQueryFromPath(root, originalPath, tlist, clauses);

	int i = 0;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		RelationShard *rs = CitusMakeNode(RelationShard);
		rs->relationId = rte->relid;
		rs->shardId = shardInterval->shardId;

		Query *qc = copyObject(q);
		UpdateRelationToShardNames((Node *) qc, list_make1(rs));

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


static uint32
ColocationGroupForDistributedUnion(Path *path)
{
	Assert(IsDistributedUnion(path));
	CustomPath *distUnion = castNode(CustomPath, path);
	/* TODO actually retreive the right colocation id for the Distributed Union */
	return 1;
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
		pathCell->data.ptr_value = WrapTableAccessWithDistributedUnion(originalPath, rte);
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
