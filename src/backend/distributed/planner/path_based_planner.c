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
static CustomPath * WrapTableAccessWithDistributedUnion(Path *originalPath, uint32 colocationId);
static Index VarnoFromFirstTargetEntry(List *tlist);
static Query * GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses);
static List * ShardIntervalListToRelationShardList(List *shardIntervalList);
static Path * OptimizeJoinPath(Path *originalPath);
static bool CanOptimizeHashPath(HashPath *hpath);
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
WrapTableAccessWithDistributedUnion(Path *originalPath, uint32 colocationId)
{
	DistributedUnionPath *distUnion = (DistributedUnionPath *)
		newNode(sizeof(DistributedUnionPath), T_CustomPath);

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
	distUnion->colocationId = colocationId;

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
PathBasedPlannerRelationHook(PlannerInfo *root,
							 RelOptInfo *relOptInfo,
							 Index restrictionIndex,
							 RangeTblEntry *rte)
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
		pathCell->data.ptr_value =
			WrapTableAccessWithDistributedUnion(originalPath,
												TableColocationId(rte->relid));
	}
}

static bool
CanOptimizeHashPath(HashPath *hpath)
{
	if (!(IsDistributedUnion(hpath->jpath.innerjoinpath) &&
		  IsDistributedUnion(hpath->jpath.outerjoinpath)))
	{
		/* can only optimize joins when both inner and outer are a distributed union */
		return false;
	}

	DistributedUnionPath *innerDU = (DistributedUnionPath *) hpath->jpath.innerjoinpath;
	DistributedUnionPath *outerDU = (DistributedUnionPath *) hpath->jpath.outerjoinpath;

	if (innerDU->colocationId != outerDU->colocationId)
	{
		/* Distributed Union is not on the same colocation group */
		return false;
	}

	/* TODO check if join is on distribution column, assume for now */
	return true;
}


static Path *
OptimizeJoinPath(Path *originalPath)
{
	if (IsA(originalPath, HashPath))
	{
		HashPath *hpath = castNode(HashPath, originalPath);
		if (CanOptimizeHashPath(hpath))
		{
			/* we can only optimize the Distributed union if the colocationId's are the same, taking any would suffice */
			uint32 colocationId = ((DistributedUnionPath *) hpath->jpath.innerjoinpath)->colocationId;

			hpath->jpath.innerjoinpath = ((DistributedUnionPath *)hpath->jpath.innerjoinpath)->worker_path;
			hpath->jpath.outerjoinpath = ((DistributedUnionPath *)hpath->jpath.outerjoinpath)->worker_path;

			/* TODO update costs of hashjoin, very naife removal of DU cost for now */
			hpath->jpath.path.startup_cost -= 2000; /* remove the double dist union cost */
			hpath->jpath.path.total_cost -= 2000; /* remove the double dist union cost */

			return (Path *) WrapTableAccessWithDistributedUnion((Path *) hpath, colocationId);
		}
	}

	return originalPath;
}


void
PathBasedPlannerJoinHook(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *outerrel,
						 RelOptInfo *innerrel,
						 JoinType jointype,
						 JoinPathExtraData *extra)
{
	if (jointype == JOIN_INNER)
	{
		ListCell *pathCell = NULL;
		foreach(pathCell, joinrel->pathlist)
		{
			Path *originalPath = lfirst(pathCell);
			pathCell->data.ptr_value = OptimizeJoinPath(originalPath);
		}
	}
}

/*
 * varno_mapping is an array where the index is the varno in the original query, or 0 if
 * no mapping is required.
 */
static Node *
VarNoMutator(Node *expr, Index *varno_mapping)
{
	if (expr == NULL)
	{
		return NULL;
	}

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


typedef struct PathQueryInfo
{
	/*
	 * Keep track of the mapping of varno's from the original query to the new query.
	 * This will be used to update the Varno attributes of Var's in the quals and target
	 * list.
	 */
	Index *varno_mapping;
} PathQueryInfo;

static void
ApplyPathToQuery(PlannerInfo *root, Query *query, Path *path, PathQueryInfo *info)
{
	switch (path->pathtype)
	{
		case T_IndexScan:
		case T_SeqScan:
		{
			/*
			 * Add table as source to the range table and keep track of the mapping with
			 * the original query
			 */
			Index scan_relid = path->parent->relid;
			Index rteIndex = info->varno_mapping[scan_relid];

			if (rteIndex == 0)
			{
				/* not added before, add and keep reference to which entry it has been added */
				RangeTblEntry *rte = root->simple_rte_array[scan_relid];
				query->rtable = lappend(query->rtable, rte);
				rteIndex = list_length(query->rtable);
				info->varno_mapping[scan_relid] = rteIndex;
			}

			/* add to from list */
			RangeTblRef *rr = makeNode(RangeTblRef);
			rr->rtindex = rteIndex;
			query->jointree->fromlist = lappend(query->jointree->fromlist, rr);

			List *quals = NIL;
			RestrictInfo *rinfo = NULL;
			foreach_ptr(rinfo, path->parent->baserestrictinfo)
			{
				Node *clause = (Node *) rinfo->clause;
				quals = lappend(quals, clause);
			}
			if (list_length(quals) > 0)
			{
				Node *qualsAnd = (Node *) make_ands_explicit(quals);
				query->jointree->quals = make_and_qual(query->jointree->quals, qualsAnd);
			}

			break;
		}

		case T_HashJoin:
		{
			JoinPath *jpath = (JoinPath *) path;

			/* add both join paths to the query */
			ApplyPathToQuery(root, query, jpath->outerjoinpath, info);
			ApplyPathToQuery(root, query, jpath->innerjoinpath, info);


			List *quals = NIL;
			RestrictInfo *rinfo = NULL;
			foreach_ptr(rinfo, jpath->joinrestrictinfo)
			{
				Node *clause = (Node *) rinfo->clause;
				quals = lappend(quals, clause);
			}
			if (list_length(quals) > 0)
			{
				Node *qualsAnd = (Node *) make_ands_explicit(quals);
				query->jointree->quals = make_and_qual(query->jointree->quals, qualsAnd);
			}

			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unknow path type in worker query"),
							errdetail("cannot turn worker path into query due to unknown "
									  "path type in plan. pathtype: %d", path->pathtype))
			);
		}
	}
}


static Query *
GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses)
{
	PathQueryInfo info = { 0 };
	info.varno_mapping = palloc0(sizeof(Index) * root->simple_rel_array_size);

	Query *q = makeNode(Query);
	q->commandType = CMD_SELECT;
	q->jointree = makeNode(FromExpr);
	ApplyPathToQuery(root, q, path, &info);


	/* copy the target list with mapped varno values to reflect the tables we are selecting */
	List *newTargetList = (List *) VarNoMutator((Node *) tlist, info.varno_mapping);

	q->targetList = newTargetList;

	List *quals = NIL;

	RestrictInfo *rinfo = NULL;
	foreach_ptr(rinfo, clauses)
	{
		Node *clause = (Node *) rinfo->clause;
		quals = lappend(quals, clause);
	}

	if (list_length(quals) > 0)
	{
		Node *qualsAnd = (Node *) make_ands_explicit(quals);
		q->jointree->quals = make_and_qual(q->jointree->quals, qualsAnd);
	}
	q->jointree->quals = VarNoMutator(q->jointree->quals, info.varno_mapping);

	return q;
}


static Index
VarnoFromFirstTargetEntry(List *tlist)
{
	TargetEntry *entry = linitial_node(TargetEntry, tlist);
	Var *var = castNode(Var, entry->expr);
	return var->varno;
}
