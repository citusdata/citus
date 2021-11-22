/* */

/* Created by Nils Dijk on 17/01/2020. */
/* */
#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type_d.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/intermediate_result_pruning.h"
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
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

typedef List *(*optimizeFn)(PlannerInfo *root, Path *originalPath);

typedef struct DistributedUnionPath
{
	CustomPath custom_path;

	/* path to be executed on the worker */
	Path *worker_path;

	uint32 colocationId;
	Expr *partitionValue;

	/*
	 * \due to a misabstraction in citus we need to keep track of a relation id that this
	 * union maps to. Idealy we would perform our pruning actions on the colocation id but
	 * we need a shard.
	 */
	Oid sampleRelid;
} DistributedUnionPath;

typedef struct RepartitionPath
{
	CustomPath custom_path;

	uint32 targetColocationId;
} RepartitionPath;

typedef struct GeoScanPath
{
	CustomPath custom_path;

	RangeTblEntry *rte;
} GeoScanPath;

static Plan * CreateDistributedUnionPlan(PlannerInfo *root, RelOptInfo *rel, struct
										 CustomPath *best_path, List *tlist,
										 List *clauses, List *custom_plans);
static List * ReparameterizeDistributedUnion(PlannerInfo *root, List *custom_private,
											 RelOptInfo *child_rel);
static CustomPath * WrapTableAccessWithDistributedUnion(Path *originalPath, uint32
														colocationId,
														Expr *partitionValue, Oid
														sampleRelid, List *subPaths);
static Path * CreateRepartitionNode(uint32 colocationId, Path *worker_path);
static Query * GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist,
								List *clauses, Index **varnoMapping);
static List * ShardIntervalListToRelationShardList(List *shardIntervalList);
static List * OptimizeJoinPath(PlannerInfo *root, Path *originalPath);
static List * OptimizeRepartitionInnerJoinPath(PlannerInfo *root, Path *originalPath);
static List * BroadcastOuterJoinPath(PlannerInfo *root, Path *originalPath);
static List * BroadcastInnerJoinPath(PlannerInfo *root, Path *originalPath);
static List * GeoOverlapJoin(PlannerInfo *root, Path *originalPath);
static Path * CreateReadIntermediateResultPath(const Path *originalPath);
static bool IsDistributedUnion(Path *path, bool recurseTransparent,
							   DistributedUnionPath **out);
static Expr * ExtractPartitionValue(List *restrictionList, Var *partitionKey);
static List * ShardIntervalListForRelationPartitionValue(Oid relationId,
														 Expr *partitionValue);
static void PathBasedPlannerGroupAgg(PlannerInfo *root, RelOptInfo *input_rel,
									 RelOptInfo *output_rel, void *extra);
static Path * OptimizeGroupAgg(PlannerInfo *root, Path *originalPath);
static bool CanOptimizeAggPath(PlannerInfo *root, AggPath *apath);
static GeoScanPath * makeGeoScanPath(Relation rel, RelOptInfo *parent,
									 PathTarget *pathtarget, double rows);
static bool IsGeoScanPath(CustomPath *path);
static RangeTblEntry * makeRangeTableEntryForRelation(Relation rel,
													  int lockmode,
													  Alias *alias,
													  bool inh,
													  bool inFromCl);


/*
 * TODO some optimizations are useless if others are already provided. This might cause
 * excessive path creation causing performance problems. Depending on the amount of
 * optimizations to be added we can keep a bitmask indicating for every entry to skip if
 * the index of a preceding successful optimization is in the bitmap.
 */
bool EnableBroadcastJoin = true;

/* list of functions that will be called to optimized in the joinhook*/
static optimizeFn joinOptimizations[] = {
	OptimizeJoinPath,
	OptimizeRepartitionInnerJoinPath,

/*	BroadcastOuterJoinPath, */
/*	BroadcastInnerJoinPath, */
/*	GeoOverlapJoin, */
};

const CustomPathMethods geoScanMethods = {
	.CustomName = "GeoScan",
};

const CustomPathMethods distributedUnionMethods = {
	.CustomName = "Distributed Union",
	.PlanCustomPath = CreateDistributedUnionPlan,
	.ReparameterizeCustomPathByChild = ReparameterizeDistributedUnion
};

const CustomPathMethods repartitionMethods = {
	.CustomName = "Repartition",
};


static CustomPath *
WrapTableAccessWithDistributedUnion(Path *originalPath, uint32 colocationId,
									Expr *partitionValue, Oid sampleRelid, List *subPaths)
{
	DistributedUnionPath *distUnion = (DistributedUnionPath *)
									  newNode(sizeof(DistributedUnionPath), T_CustomPath);

	distUnion->custom_path.path.pathtype = T_CustomScan;
	distUnion->custom_path.path.parent = originalPath->parent;
	distUnion->custom_path.path.pathtarget = originalPath->pathtarget;
	distUnion->custom_path.path.param_info = originalPath->param_info;

	/* TODO use a better cost model */
	distUnion->custom_path.path.rows = originalPath->rows;
	distUnion->custom_path.path.startup_cost = originalPath->startup_cost + 1000;
	distUnion->custom_path.path.total_cost = originalPath->total_cost + 1000;

	distUnion->custom_path.methods = &distributedUnionMethods;

	distUnion->worker_path = originalPath;
	distUnion->custom_path.custom_private = list_make1(originalPath);
	distUnion->colocationId = colocationId;
	distUnion->partitionValue = partitionValue;
	distUnion->sampleRelid = sampleRelid;
	distUnion->custom_path.custom_paths = subPaths;

	return (CustomPath *) distUnion;
}


typedef struct TransformVarToParamExternMutatorContext
{
	PlannerInfo *root;
	Index *varnoMapping;
} TransformVarToParamExternMutatorContext;


static Node *
TransformVarToParamExternMutator(Node *node,
								 TransformVarToParamExternMutatorContext *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		Index originalVarNo = context->varnoMapping[var->varno];
		if (originalVarNo == 0)
		{
			/* no mapping was required */
			originalVarNo = var->varno;
		}
		Assert(originalVarNo > 0);

		/* If not to be replaced, we can just return the Var unmodified */
		if (!bms_is_member(originalVarNo, context->root->curOuterRels))
		{
			return (Node *) var;
		}

		Param *paramExec = replace_nestloop_param_var(context->root, var);

		/* TODO: figure out which Var's to replace by which parameters*/
		/* TODO: hack - insert param 1 for now */
		Param *paramExtern = makeNode(Param);
		paramExtern->paramkind = PARAM_EXTERN;

		/* Exec is 0-index, Extern is 1-indexed */
		paramExtern->paramid = paramExec->paramid + 1;

		paramExtern->paramtype = paramExec->paramtype;
		paramExtern->paramtypmod = paramExec->paramtypmod;
		paramExtern->paramcollid = paramExec->paramcollid;
		paramExtern->location = paramExec->location;

		return (Node *) paramExtern;
	}
	else if (IsA(node, Query))
	{
		return (Node *) query_tree_mutator((Query *) node,
										   TransformVarToParamExternMutator,
										   (void *) context,
										   0);
	}

	return expression_tree_mutator(node, TransformVarToParamExternMutator,
								   (void *) context);
}


static Query *
TransformVarToParamExtern(Query *query, PlannerInfo *root, Index *varnoMapping)
{
	TransformVarToParamExternMutatorContext context = {
		root,
		varnoMapping
	};

	return castNode(Query, TransformVarToParamExternMutator((Node *) query, &context));
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

	Index *varnoMapping = NULL; /* store mapping back for outerrel checks */
	Query *q = GetQueryFromPath(root, distUnion->worker_path, tlist, clauses,
								&varnoMapping);

	/*
	 * Assume shards are colocated, any shard should suffice for now to find the initial
	 * interval list
	 */

	/* TODO track colocation information on the Distributed Union node to fetch required information in a more optimal setting*/
	List *shardIntervalList = ShardIntervalListForRelationPartitionValue(
		distUnion->sampleRelid,
		distUnion->partitionValue);

	int i = 0;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		List *colocatedShards = ColocatedShardIntervalList(shardInterval);
		List *relationShardList = ShardIntervalListToRelationShardList(colocatedShards);

		Query *qc = copyObject(q);
		UpdateRelationToShardNames((Node *) qc, relationShardList);

		/* transform Var's for other varno's to parameters */
		qc = TransformVarToParamExtern(qc, root, varnoMapping);

		StringInfoData buf;
		initStringInfo(&buf);
		pg_get_query_def(qc, &buf);

		Task *sqlTask = CreateBasicTask(workerJob->jobId, i, READ_TASK, buf.data);
		sqlTask->anchorShardId = shardInterval->shardId;
		sqlTask->taskPlacementList = ActiveShardPlacementList(shardInterval->shardId);
		workerJob->taskList = lappend(workerJob->taskList, sqlTask);
		i++;
	}
	workerJob->jobQuery = q;

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = workerJob;
	distributedPlan->modLevel = ROW_MODIFY_READONLY;
	distributedPlan->relationIdList = list_make1_oid(distUnion->sampleRelid);
	distributedPlan->expectResults = true;

	Plan *subPlan = NULL;
	int subPlanCount = 0;
	foreach_ptr(subPlan, custom_plans)
	{
		PlannedStmt *result = makeNode(PlannedStmt);
		result->commandType = CMD_SELECT;
		result->planTree = subPlan;
		List *rtable = NIL;
		for (i = 1; i < root->simple_rel_array_size; i++)
		{
			RangeTblEntry *rte = root->simple_rte_array[i];
			rtable = lappend(rtable, rte);
		}
		rtable = lappend(rtable, root->simple_rte_array[1]);
		result->rtable = rtable;

		/* 1 indexed */
		subPlanCount++;
		DistributedSubPlan *dsubPlan = CitusMakeNode(DistributedSubPlan);
		dsubPlan->plan = result;
		dsubPlan->subPlanId = subPlanCount;

		distributedPlan->subPlanList = lappend(distributedPlan->subPlanList, dsubPlan);
	}

	distributedPlan->usedSubPlanNodeList = FindSubPlanUsages(distributedPlan);

	CustomScan *plan = makeNode(CustomScan);
	plan->scan.scanrelid = 0;
	plan->custom_scan_tlist = tlist;
	plan->flags = best_path->flags;
	plan->methods = &AdaptiveExecutorCustomScanMethods;
	plan->custom_private = list_make1(distributedPlan);
	plan->custom_plans = custom_plans;

	plan->scan.plan.targetlist = tlist;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

/*	plan->scan.plan.qual = clauses; */
/*	plan->custom_exprs = clauses; */

	return (Plan *) plan;
}


static List *
ShardIntervalListForRelationPartitionValue(Oid relationId, Expr *partitionValue)
{
	if (partitionValue && IsA(partitionValue, Const))
	{
		/* prune shard list to target */
		Const *partitionValueConst = castNode(Const, partitionValue);

		/* TODO assert the constant is of the correct value */
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		return list_make1(FindShardInterval(partitionValueConst->constvalue, cacheEntry));
	}

	/* all shards */
	return LoadShardIntervalList(relationId);
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
 *
 * If recurseTransparent is set it will recurse into transparant nodes like Materialize
 *
 * If out is set to not NULL it will write the pointer to the union at the location
 * specified
 */
static bool
IsDistributedUnion(Path *path, bool recurseTransparent, DistributedUnionPath **out)
{
	if (recurseTransparent)
	{
		switch (nodeTag(path))
		{
			case T_MaterialPath:
			{
				MaterialPath *materialPath = castNode(MaterialPath, path);
				return IsDistributedUnion(materialPath->subpath, recurseTransparent, out);
			}

			default:
			{
				break;
			}
		}
	}

	if (!IsA(path, CustomPath))
	{
		return false;
	}

	CustomPath *cpath = castNode(CustomPath, path);
	if (cpath->methods != &distributedUnionMethods)
	{
		return false;
	}

	if (out != NULL)
	{
		*out = (DistributedUnionPath *) cpath;
	}

	return true;
}


void
PathBasedPlannerRelationHook(PlannerInfo *root,
							 RelOptInfo *relOptInfo,
							 Index restrictionIndex,
							 RangeTblEntry *rte)
{
	if (!IsCitusTable(rte->relid))
	{
		/* table accessed is not distributed, no paths to change */
		return;
	}

	Var *partitionKey = DistPartitionKey(rte->relid);
	Expr *partitionValue = NULL;

	/* the distirbuted table has a partition key, lets check filters if there is a value */
	if (partitionKey != NULL)
	{
		/* use the first rel id included in this relation */
		partitionKey->varno = bms_next_member(relOptInfo->relids, -1);
		Assert(bms_num_members(relOptInfo->relids) == 1);

		partitionValue = ExtractPartitionValue(relOptInfo->baserestrictinfo,
											   partitionKey);
	}

	/* wrap every path with a distributed union custom path */
	ListCell *pathCell = NULL;
	foreach(pathCell, relOptInfo->pathlist)
	{
		Path *originalPath = lfirst(pathCell);
		CustomPath *wrappedPath = WrapTableAccessWithDistributedUnion(
			originalPath,
			TableColocationId(rte->relid),
			partitionValue,
			rte->relid,
			NIL);
		SetListCellPtr(pathCell, wrappedPath);
	}

	/* hardcoded hack for adding geo distributed tables as an alternative path */
	Relation rel = relation_open(rte->relid, AccessShareLock);
	if (UseGeoPartitioning && strcmp(RelationGetRelationName(rel),
									 "belgium_planet_osm_roads_dist") == 0)
	{
		if (OnlyGeoPartitioning)
		{
			/* makes the geo path the only path to access the relation */
			relOptInfo->pathlist = NIL;
		}


		Oid geoRelid = RelnameGetRelid("belgium_planet_osm_roads_geo");
		Relation georel = relation_open(geoRelid, AccessShareLock);

		Path *geoPath = (Path *) makeGeoScanPath(georel,
												 relOptInfo,
												 relOptInfo->reltarget,
												 relOptInfo->rows);
		geoPath = (Path *)
				  WrapTableAccessWithDistributedUnion(geoPath,
													  TableColocationId(geoRelid),
													  NULL,
													  geoRelid,
													  NIL);

		if (EnableGeoPartitioningGrouping)
		{
			/* verymuch just an int4 at the moment */
			SortGroupClause *sgc = makeNode(SortGroupClause);
			sgc->tleSortGroupRef = 1; /* should be first field */
			sgc->eqop = 96;
			sgc->sortop = 97;
			sgc->nulls_first = false;
			sgc->hashable = true; /* ? just assume an in can be hashed */

			List *groupClause = list_make1(sgc);

			/* creating the target list */
			PathTarget *groupPathTarget = create_empty_pathtarget();
			int numAggs = 0;
			Expr *expr = NULL;
			foreach_ptr(expr, relOptInfo->reltarget->exprs)
			{
				if (!IsA(expr, Var))
				{
					continue;
				}
				Var *var = castNode(Var, expr);

				switch (var->varattno)
				{
					case 1: /* k */
					{
						/* transparently add grouping keys */
						add_column_to_pathtarget(groupPathTarget, expr, 0);

						break;
					}

					case 2: /* osm_id */
					{
						/* wrapping non partitioned columns and non-primary keys in any_value */
						Aggref *aggref = makeNode(Aggref);
						aggref->aggfnoid = 18333; /* any_value */
						aggref->aggtype = var->vartype;
						aggref->aggtranstype = var->vartype;
						aggref->aggfilter = NULL;
						aggref->aggstar = false;
						aggref->aggvariadic = false;
						aggref->aggkind = AGGKIND_NORMAL;
						aggref->aggsplit = AGGSPLIT_SIMPLE;
						aggref->location = 0;

						aggref->args = list_make1(
							makeTargetEntry((Expr *) var, 1, NULL, false));
						TargetEntry *argTLE = NULL;
						foreach_ptr(argTLE, aggref->args)
						{
							aggref->aggargtypes =
								lappend_oid(aggref->aggargtypes,
											exprType((Node *) argTLE->expr));
						}
						add_column_to_pathtarget(groupPathTarget, (Expr *) aggref, 0);
						numAggs++;

						break;
					}

					case 3: /* way */
					{
						/* reconstruct partitioned values via ST_Union() */

						Aggref *aggref = makeNode(Aggref);
						aggref->aggfnoid = 16861; /* ST_Union */
						aggref->aggtype = 16390;
						aggref->aggtranstype = 2281;
						aggref->aggfilter = NULL;
						aggref->aggstar = false;
						aggref->aggvariadic = false;
						aggref->aggkind = AGGKIND_NORMAL;
						aggref->aggsplit = AGGSPLIT_SIMPLE;
						aggref->location = 0;

						aggref->args = list_make1(makeTargetEntry(expr, 1, NULL, false));
						TargetEntry *argTLE = NULL;
						foreach_ptr(argTLE, aggref->args)
						{
							aggref->aggargtypes =
								lappend_oid(aggref->aggargtypes,
											exprType((Node *) argTLE->expr));
						}
						add_column_to_pathtarget(groupPathTarget, (Expr *) aggref, 0);
						numAggs++;
					}
				}
			}

			/* TODO figure out costing for our grouping */
			AggClauseCosts costs = {
#if PG_VERSION_NUM < 140000
				.numAggs = numAggs,
				.numOrderedAggs = 0,
				.hasNonPartial = false,
				.hasNonSerial = false,
#endif

				.transCost.startup = 0,
				.transCost.per_tuple = 0,
				.finalCost.startup = 0,
				.finalCost.per_tuple = 0,

				.transitionSpace = 0,
			};

			geoPath = (Path *) create_agg_path(root,
											   relOptInfo,
											   geoPath,
											   groupPathTarget,
											   AGG_HASHED,
											   AGGSPLIT_SIMPLE,
											   groupClause,
											   NIL, &costs,
											   2);
		}


		add_path(relOptInfo, geoPath);

		relation_close(georel, AccessShareLock);
	}
	relation_close(rel, AccessShareLock);
}


static GeoScanPath *
makeGeoScanPath(Relation rel, RelOptInfo *parent, PathTarget *pathtarget, double rows)
{
	GeoScanPath *geoPath = (GeoScanPath *) newNode(sizeof(GeoScanPath), T_CustomPath);
	CustomPath *cpath = (CustomPath *) geoPath;
	Path *path = (Path *) geoPath;

	path->pathtype = T_CustomScan;
	path->parent = parent;

	PathTarget *targetCopy = create_empty_pathtarget();
	Expr *expr = NULL;
	foreach_ptr(expr, pathtarget->exprs)
	{
		bool isPrimaryKey = false;
		if (IsA(expr, Var))
		{
			/* TODO assume the first attribute of a relation as its PK */
			Var *var = (Var *) expr;
			isPrimaryKey = var->varattno == 1;
		}

		/*
		 * Geo partitioning cuts the geometry of the distibution column into pieces, they
		 * need to be reconstructed by grouping on the primary key. Add the primary keys
		 * to a grouping set with reference 1
		 */
		add_column_to_pathtarget(targetCopy, expr,
								 isPrimaryKey ? 1 : 0);
	}

	path->pathtarget = targetCopy;
	path->param_info = NULL;

	path->rows = rows * 1.2; /* add 20% for the duplication */
	path->startup_cost = 0;
	path->total_cost = 0;

	cpath->methods = &geoScanMethods;

	geoPath->rte = makeRangeTableEntryForRelation(rel, AccessShareLock, NULL, false,
												  true);

	return geoPath;
}


static bool
IsGeoScanPath(CustomPath *path)
{
	return path->methods == &geoScanMethods;
}


static RangeTblEntry *
makeRangeTableEntryForRelation(Relation rel,
							   int lockmode,
							   Alias *alias,
							   bool inh,
							   bool inFromCl)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	char *refname = alias ? alias->aliasname : RelationGetRelationName(rel);

	Assert(lockmode == AccessShareLock ||
		   lockmode == RowShareLock ||
		   lockmode == RowExclusiveLock);
	Assert(CheckRelationLockedByMe(rel, lockmode, true));

	rte->rtekind = RTE_RELATION;
	rte->alias = alias;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = lockmode;

	/*
	 * Build the list of effective column names using user-supplied aliases
	 * and/or actual column names.
	 */
	rte->eref = makeAlias(refname, NIL);
	rte->eref->colnames = list_make3(makeString("k"),
									 makeString("osm_id"),
									 makeString("way"));

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = inh;
	rte->inFromCl = inFromCl;

	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid;  /* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;
	rte->extraUpdatedCols = NULL;

	return rte;
}


static Expr *
ExtractPartitionValue(List *restrictionList, Var *partitionKey)
{
	RestrictInfo *info = NULL;
	foreach_ptr(info, restrictionList)
	{
		if (!NodeIsEqualsOpExpr((Node *) info->clause))
		{
			continue;
		}

		/* equality operator, check for partition column */
		OpExpr *eq = castNode(OpExpr, info->clause);
		Expr *left = list_nth(eq->args, 0);
		Expr *right = list_nth(eq->args, 1);

		if (IsA(left, Var))
		{
			Var *leftVar = castNode(Var, left);
			if (leftVar->varno == partitionKey->varno &&
				leftVar->varattno == partitionKey->varattno)
			{
				/* partition column, return right*/
				return right;
			}
		}

		if (IsA(right, Var))
		{
			Var *rightVar = castNode(Var, left);
			if (rightVar->varno == partitionKey->varno &&
				rightVar->varattno == partitionKey->varattno)
			{
				/* partition column, return left */
				return left;
			}
		}
	}

	return NULL;
}


static NameData
GetFunctionNameData(Oid funcid)
{
	HeapTuple proctup = NULL;
	Form_pg_proc procform = NULL;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
	}
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* copy name by value */
	NameData result = procform->proname;

	ReleaseSysCache(proctup);

	return result;
}


typedef struct GeoJoinPathMatch
{
	Const *stdwithinDistanceConst;

/*	AggPath *innerGrouping; */
	DistributedUnionPath *innerDistUnion;
	GeoScanPath *innerPath;

/*	AggPath *outerGrouping; */
	DistributedUnionPath *outerDistUnion;
	GeoScanPath *outerPath;
} GeoJoinPathMatch;


#include "distributed/planner/pattern_match.h"
static List *
GeoOverlapJoin(PlannerInfo *root, Path *originalPath)
{
	GeoJoinPathMatch match = { 0 };

	bool didMatch = false;

	/*
	 * temporary nest the matcher till we figure out the final grouping, for now we need
	 * to be able to toggle between
	 */
	if (EnableGeoPartitioningGrouping)
	{
		IfPathMatch(
			originalPath,
			MatchJoin(
				NoCapture,
				JOIN_INNER,

				/* match on join restriction info */
				MatchJoinRestrictions(
					NoCapture,
					MatchExprNamedOperation(
						NoCapture,
						geometry_overlaps,
						MatchVar(NoCapture),
						MatchExprNamedFunction(
							NoCapture,
							st_expand,
							MatchVar(NoCapture),
							MatchConst(
								&match.stdwithinDistanceConst,
								MatchFields(consttype == FLOAT8OID))))),

				/* match inner path in join */
				SkipReadThrough(
					NoCapture,
					MatchGrouping(
						NoCapture, /*&match.innerGrouping,*/
						MatchDistributedUnion(
							&match.innerDistUnion,
							MatchGeoScan(
								&match.innerPath)))),

				/* match outer path in join */
				SkipReadThrough(
					NoCapture,
					MatchGrouping(
						NoCapture, /*&match.outerGrouping,*/
						MatchDistributedUnion(&match.outerDistUnion,
											  MatchGeoScan(
												  &match.outerPath))))))
		{
			didMatch = true;
		}
	}
	else
	{
		IfPathMatch(
			originalPath,
			MatchJoin(
				NoCapture,
				JOIN_INNER,

				/* match on join restriction info */
				MatchJoinRestrictions(
					NoCapture,
					MatchExprNamedOperation(
						NoCapture,
						geometry_overlaps,
						MatchVar(NoCapture),
						MatchExprNamedFunction(
							NoCapture,
							st_expand,
							MatchVar(NoCapture),
							MatchConst(
								&match.stdwithinDistanceConst,
								MatchFields(consttype == FLOAT8OID))))),

				/* match inner path in join */
				SkipReadThrough(
					NoCapture,
					MatchDistributedUnion(
						&match.innerDistUnion,
						MatchGeoScan(
							&match.innerPath))),

				/* match outer path in join */
				SkipReadThrough(
					NoCapture,
					MatchDistributedUnion(&match.outerDistUnion,
										  MatchGeoScan(
											  &match.outerPath)))))
		{
			didMatch = true;
		}
	}

	if (didMatch)
	{
		/* have a match on the geo join pattern, all fields are stored in `match` */
		ereport(DEBUG1, (errmsg("distance join with distance: %f",
								DatumGetFloat8(match.stdwithinDistanceConst->constvalue)
								)));

		JoinPath *jpath = makeNode(NestPath);
		*jpath = *((JoinPath *) originalPath); /* copy basic join settings */
		jpath->path.type = T_NestPath;

		jpath->innerjoinpath = (Path *) match.innerPath;
		jpath->outerjoinpath = (Path *) match.outerPath;
/*			(Path *) create_append_path( */
/*			root, */
/*			match.outerPath->custom_path.path.parent, */
/*			list_make1(match.outerPath), / * TODO add the result of the shuffled job * / */
/*			NIL, */
/*			NIL, */
/*			NULL, */
/*			0, */
/*			false, */
/*			NIL, */
/*			match.outerPath->custom_path.path.rows + 0); */

		jpath->path.startup_cost -= 2000; /* remove the double dist union cost */
		jpath->path.total_cost -= 2000; /* remove the double dist union cost */

		/* TODO add grouping */

		Path *newPath = (Path *) WrapTableAccessWithDistributedUnion(
			(Path *) jpath,
			match.innerDistUnion->colocationId,
			match.innerDistUnion->partitionValue,
			match.innerDistUnion->sampleRelid,
			NIL); /* TODO is this ok? */

		return list_make1(newPath);
	}

	return NIL;
}


static List *
OptimizeJoinPath(PlannerInfo *root, Path *originalPath)
{
	DistributedUnionPath *innerDU = NULL;
	DistributedUnionPath *outerDU = NULL;
	JoinPath *jpath = NULL;

	IfPathMatch(
		originalPath,
		MatchJoin(
			&jpath,
			JOIN_INNER,

			/* match on join restriction info */
			MatchAny,

			/* match inner path in join */
			SkipReadThrough(
				NoCapture,
				MatchDistributedUnion(
					&innerDU,
					MatchAny)),

			/* match outer path in join */
			SkipReadThrough(
				NoCapture,
				MatchDistributedUnion(
					&outerDU,
					MatchAny))))
	{
		if (innerDU->colocationId != outerDU->colocationId)
		{
			/* Distributed Union is not on the same colocation group */
			return NIL;
		}

		if (!equal(innerDU->partitionValue, outerDU->partitionValue))
		{
			/* TODO this is most likely too strict, but if the values are strictly the same we can easily take one during merging */
			return NIL;
		}

		const DistributedUnionPath *baseDistUnion = innerDU;

		JoinPath *jcpath = makeNode(NestPath);
		*jcpath = *jpath;
		jcpath->path.type = T_NestPath;

		jcpath->innerjoinpath = innerDU->worker_path;
		jcpath->outerjoinpath = outerDU->worker_path;

		/* TODO update costs of hashjoin, very naive removal of DU cost for now */
		jcpath->path.startup_cost -= 2000; /* remove the double dist union cost */
		jcpath->path.total_cost -= 2000; /* remove the double dist union cost */

		Path *newPath = (Path *) WrapTableAccessWithDistributedUnion(
			(Path *) jcpath,
			baseDistUnion->colocationId,
			baseDistUnion->partitionValue,
			baseDistUnion->sampleRelid,
			baseDistUnion->custom_path.custom_paths);

		return list_make1(newPath);
	}

	return NIL;
}


static List *
OptimizeRepartitionInnerJoinPath(PlannerInfo *root, Path *originalPath)
{
	DistributedUnionPath *innerDU = NULL;
	DistributedUnionPath *outerDU = NULL;
	JoinPath *joinPath = NULL;

	/*
	 * Match the following shape:
	 *
	 *                    +---------+
	 *                    |  Join   |
	 *                    +---------+
	 *                   /           \
	 *  +---------------------+ +---------------------+
	 *  | Collect             | | Collect             |
	 *  | - ColocationID: $1  | | - ColocationID: !$1 |
	 *  +---------------------+ +---------------------+
	 *
	 */
	IfPathMatch(
		originalPath,
		MatchJoin(
			&joinPath,
			JOIN_INNER,

			/* match on join restriction info */
			MatchAny,

			/* match inner path in join */
			SkipReadThrough(
				NoCapture,
				MatchDistributedUnion(
					&innerDU,
					MatchAny)),

			/* match outer path in join */
			SkipReadThrough(
				NoCapture,
				MatchDistributedUnion(
					&outerDU,
					MatchAny))))
	{
		/*
		 * We matched the shape of our join. Next we need to verify the join is not
		 * already colocated, because a colocated join can always push down. To verify
		 * colocatededness of the join we need to verify the following:
		 *  - the join is happening in the same colocation id
		 *  - there is an equivalence on the list of distribution columns on both the
		 *    inner and the outer part of the join.
		 * If any of the above do not satisfy the joins are not colocated
		 */

		if (innerDU->colocationId == outerDU->colocationId)
		{
			/* TODO check skipped the equivalence check between distribution attributes */
			return NIL;
		}

		/*
		 * We want to repartition the inner join to the colocation of the outer join. For
		 * this we need to understand which attribute on the inner join has an equivalence
		 * condition on any of the attributes in the outer part of the join.
		 *
		 * Once we know on which attribute to repartition the inner part we can create a
		 * new tree in the following shape:
		 *    +------------------------------------+
		 *    | Collect                            |
		 *    | - ColocationID: outer.ColocationID |
		 *    +------------------------------------+
		 *                      |
		 *                 +---------+
		 *                 |  Join   |
		 *                 +---------+
		 *                /           \
		 *  +-------------------+  +------------------------------------+
		 *  | oruter.worke_path |  | Repartition                        |
		 *  +-------------------+  | - ColocationID: outer.colocationID |
		 *                         +------------------------------------+
		 *                                            |
		 *                                  +-------------------+
		 *                                  | inner.worker_path |
		 *                                  +-------------------+
		 */

		/* create new Join node */
		JoinPath *newJoinPath = makeNode(NestPath);
		*newJoinPath = *joinPath;
		newJoinPath->path.type = T_NestPath; /* reset type after copied join data */

		/* populate outer path*/
		newJoinPath->outerjoinpath = outerDU->worker_path;

		/* TODO understand how to describe on which attribute the Repartition needs to happen */
		newJoinPath->innerjoinpath = CreateRepartitionNode(outerDU->colocationId,
														   innerDU->worker_path);

		/* TODO find a good way to calculate join costs based on its inner/outer paths */
		/* subtract the double collect cost */
		newJoinPath->path.startup_cost -= 2000;
		newJoinPath->path.total_cost -= 2000;

		/* add the costs for the repartition */
		newJoinPath->path.startup_cost += 500;
		newJoinPath->path.total_cost += 500;

		/* create Collect on top of new join, base Collect on matched outer Collect */
		const DistributedUnionPath *baseDistUnion = outerDU;
		Path *newPath = (Path *) WrapTableAccessWithDistributedUnion(
			(Path *) newJoinPath,
			baseDistUnion->colocationId,
			baseDistUnion->partitionValue,
			baseDistUnion->sampleRelid,
			baseDistUnion->custom_path.custom_paths);

		return list_make1(newPath);
	}

	return NIL;
}


static Path *
CreateRepartitionNode(uint32 colocationId, Path *worker_path)
{
	RepartitionPath *repartition = (RepartitionPath *)
								   newNode(sizeof(RepartitionPath), T_CustomPath);

	repartition->custom_path.path.pathtype = T_CustomScan;
	repartition->custom_path.path.parent = worker_path->parent;
	repartition->custom_path.path.pathtarget = worker_path->pathtarget;
	repartition->custom_path.path.param_info = worker_path->param_info;

	/* TODO use a better cost model */
	repartition->custom_path.path.rows = worker_path->rows;
	repartition->custom_path.path.startup_cost = worker_path->startup_cost + 500;
	repartition->custom_path.path.total_cost = worker_path->total_cost + 500;

	repartition->custom_path.methods = &repartitionMethods;

	repartition->custom_path.custom_private = list_make1(worker_path);
	repartition->targetColocationId = colocationId;

	return (Path *) repartition;
}


static List *
BroadcastOuterJoinPath(PlannerInfo *root, Path *originalPath)
{
	if (!EnableBroadcastJoin)
	{
		return NIL;
	}

	switch (originalPath->pathtype)
	{
		case T_NestLoop:
		case T_HashJoin:
		{
			const JoinPath *jpath = (JoinPath *) originalPath;
			List *newPaths = NIL;

			if (IsDistributedUnion(jpath->outerjoinpath, false, NULL))
			{
				/* broadcast inner join path */
				DistributedUnionPath *baseDistUnion =
					(DistributedUnionPath *) jpath->outerjoinpath;

				/*
				 * Shallow copy of any join node, this does not imply executing a nested
				 * join, but the nested join contains all the information we need to send
				 * the join to the worker
				 */
				JoinPath *jcpath = makeNode(NestPath);
				*jcpath = *jpath;
				jcpath->path.type = T_NestPath;

				jcpath->outerjoinpath = baseDistUnion->worker_path;
				Path *subPath = jcpath->innerjoinpath;
				jcpath->innerjoinpath = CreateReadIntermediateResultPath(subPath);

				/* TODO update costs of hashjoin, very naife removal of DU cost for now */
				jcpath->path.startup_cost -= 1500;
				jcpath->path.total_cost -= 1500;

				Path *newPath = (Path *) WrapTableAccessWithDistributedUnion(
					(Path *) jcpath,
					baseDistUnion->colocationId,
					baseDistUnion->partitionValue,
					baseDistUnion->sampleRelid,
					lappend(list_copy(baseDistUnion->custom_path.custom_paths), subPath));
				newPaths = lappend(newPaths, newPath);
			}

			return newPaths;
		}

		default:
		{
			return NIL;
		}
	}
}


static List *
BroadcastInnerJoinPath(PlannerInfo *root, Path *originalPath)
{
	if (!EnableBroadcastJoin)
	{
		return NIL;
	}

	switch (originalPath->pathtype)
	{
		case T_NestLoop:
		case T_HashJoin:
		{
			const JoinPath *jpath = (JoinPath *) originalPath;
			List *newPaths = NIL;

			if (IsDistributedUnion(jpath->innerjoinpath, false, NULL))
			{
				/* broadcast inner join path */
				DistributedUnionPath *baseDistUnion =
					(DistributedUnionPath *) jpath->innerjoinpath;

				/*
				 * Shallow copy of any join node, this does not imply executing a nested
				 * join, but the nested join contains all the information we need to send
				 * the join to the worker
				 */
				JoinPath *jcpath = makeNode(NestPath);
				*jcpath = *jpath;
				jcpath->path.type = T_NestPath;

				jcpath->innerjoinpath = baseDistUnion->worker_path;
				Path *subPath = jcpath->outerjoinpath;
				jcpath->outerjoinpath = CreateReadIntermediateResultPath(subPath);

				/* TODO update costs of hashjoin, very naife removal of DU cost for now */
				jcpath->path.startup_cost -= 1500;
				jcpath->path.total_cost -= 1500;

				Path *newPath = (Path *) WrapTableAccessWithDistributedUnion(
					(Path *) jcpath,
					baseDistUnion->colocationId,
					baseDistUnion->partitionValue,
					baseDistUnion->sampleRelid,
					lappend(list_copy(baseDistUnion->custom_path.custom_paths), subPath));
				newPaths = lappend(newPaths, newPath);
			}

			return newPaths;
		}

		default:
		{
			return NIL;
		}
	}
}


static Path *
CreateReadIntermediateResultPath(const Path *originalPath)
{
	/* TODO might require a custom path for read intermediate result */
	Path *path = makeNode(Path);
	path->pathtype = T_FunctionScan;
	path->parent = originalPath->parent;
	path->pathtarget = originalPath->pathtarget;

	/* TODO some network cost to be modelled */
	path->total_cost = originalPath->total_cost + 500;
	path->startup_cost = originalPath->startup_cost + 500;

	return path;
}


void
PathBasedPlannerJoinHook(PlannerInfo *root,
						 RelOptInfo *joinrel,
						 RelOptInfo *outerrel,
						 RelOptInfo *innerrel,
						 JoinType jointype,
						 JoinPathExtraData *extra)
{
	/*
	 * Adding a path to a list includes lappend which might be destructive. Since we are
	 * looping over the paths we are adding to we should keep a list of new paths to add
	 * and only add them after we have found all the paths we want to add.
	 */
	List *newPaths = NIL;

	ListCell *pathCell = NULL;
	foreach(pathCell, joinrel->pathlist)
	{
		Path *originalPath = lfirst(pathCell);
		for (int i = 0; i < sizeof(joinOptimizations) / sizeof(joinOptimizations[1]); i++)
		{
			List *alternativePaths = joinOptimizations[i](root, originalPath);
			newPaths = list_concat(newPaths, alternativePaths);
		}
	}

	Path *path = NULL;
	foreach_ptr(path, newPaths)
	{
		add_path(joinrel, path);
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
			return expression_tree_mutator(expr, (void *) VarNoMutator, varno_mapping);
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
		case T_Agg:
		{
			AggPath *apath = castNode(AggPath, path);

			/* the subpath needs to be applied before we can apply the grouping clause */
			ApplyPathToQuery(root, query, apath->subpath, info);

			query->groupClause = apath->groupClause;
			break;
		}

		case T_BitmapHeapScan:
		{
			BitmapHeapPath *bpath = castNode(BitmapHeapPath, path);
			ApplyPathToQuery(root, query, bpath->bitmapqual, info);
			return;
		}

		case T_IndexScan:
		case T_IndexOnlyScan:
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

		case T_NestLoop:
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

		/* TODO temporary placeholder for read_intermediate_result*/
		case T_FunctionScan:
		{
			Oid functionOid = CitusReadIntermediateResultFuncId();

			/* result_id text */
			Const *resultIdConst = makeNode(Const);
			resultIdConst->consttype = TEXTOID;
			resultIdConst->consttypmod = -1;
			resultIdConst->constlen = -1;
			resultIdConst->constvalue = CStringGetTextDatum("0_1");
			resultIdConst->constbyval = false;
			resultIdConst->constisnull = false;
			resultIdConst->location = -1;

			/* format citus_copy_format DEFAULT 'csv'::citus_copy_format */
			Oid copyFormatId = BinaryCopyFormatId();
			Const *resultFormatConst = makeNode(Const);
			resultFormatConst->consttype = CitusCopyFormatTypeId();
			resultFormatConst->consttypmod = -1;
			resultFormatConst->constlen = 4;
			resultFormatConst->constvalue = ObjectIdGetDatum(copyFormatId);
			resultFormatConst->constbyval = true;
			resultFormatConst->constisnull = false;
			resultFormatConst->location = -1;

			/* build the call to read_intermediate_result */
			FuncExpr *funcExpr = makeNode(FuncExpr);
			funcExpr->funcid = functionOid;
			funcExpr->funcretset = true;
			funcExpr->funcvariadic = false;
			funcExpr->funcformat = 0;
			funcExpr->funccollid = 0;
			funcExpr->inputcollid = 0;
			funcExpr->location = -1;
			funcExpr->args = list_make2(resultIdConst, resultFormatConst);

			List *funcColNames = NIL;
			List *funcColTypes = NIL;
			List *funcColTypMods = NIL;
			List *funcColCollations = NIL;
			Node *expr = NULL;
			foreach_ptr(expr, path->pathtarget->exprs)
			{
				Oid colType = exprType(expr);
				Oid colCollation = exprCollation(expr);
				int32 colTypeMod = exprTypmod(expr);

				funcColNames = lappend(funcColNames, makeString("t1.b")); /* TODO resolve actual name */
				funcColTypes = lappend_oid(funcColTypes, colType);
				funcColTypMods = lappend_oid(funcColTypMods, colTypeMod);
				funcColCollations = lappend_int(funcColCollations, colCollation);
			}

			/* build the RTE for the call to read_intermediate_result */
			RangeTblFunction *rangeTableFunction = makeNode(RangeTblFunction);
			rangeTableFunction->funccolcount = list_length(funcColNames);
			rangeTableFunction->funccolnames = funcColNames;
			rangeTableFunction->funccoltypes = funcColTypes;
			rangeTableFunction->funccoltypmods = funcColTypMods;
			rangeTableFunction->funccolcollations = funcColCollations;
			rangeTableFunction->funcparams = NULL;
			rangeTableFunction->funcexpr = (Node *) funcExpr;

			Alias *funcAlias = makeNode(Alias);
			funcAlias->aliasname = "Distributed Subplan 0_1";
			funcAlias->colnames = funcColNames;

			RangeTblEntry *rangeTableEntry = makeNode(RangeTblEntry);
			rangeTableEntry->rtekind = RTE_FUNCTION;
			rangeTableEntry->functions = list_make1(rangeTableFunction);
			rangeTableEntry->inFromCl = true;
			rangeTableEntry->eref = funcAlias;

			/* add the RangeTableEntry */
			query->rtable = lappend(query->rtable, rangeTableEntry);
			Index rteIndex = list_length(query->rtable);
			Index scan_relid = path->parent->relid;
			info->varno_mapping[scan_relid] = rteIndex;

			RangeTblRef *rr = makeNode(RangeTblRef);
			rr->rtindex = rteIndex;
			query->jointree->fromlist = lappend(query->jointree->fromlist, rr);

			break;
		}

		case T_CustomScan:
		{
			if (IsGeoScanPath(castNode(CustomPath, path)))
			{
				GeoScanPath *geoPath = (GeoScanPath *) path;

				Index scan_relid = path->parent->relid;
				Index rteIndex = info->varno_mapping[scan_relid];

				if (rteIndex == 0)
				{
					RangeTblEntry *rte = geoPath->rte;
					query->rtable = lappend(query->rtable, rte);
					rteIndex = list_length(query->rtable);
					info->varno_mapping[scan_relid] = rteIndex;
				}

				/* add to from list */
				RangeTblRef *rr = makeNode(RangeTblRef);
				rr->rtindex = rteIndex;
				query->jointree->fromlist = lappend(query->jointree->fromlist, rr);

				break;
			}

			/* fallthrough to error */
		}

		default:
		{
			ereport(ERROR, (errmsg("unknown path type in worker query"),
							errdetail("cannot turn worker path into query due to unknown "
									  "path type in plan. pathtype: %d", path->pathtype))
					);
		}
	}
}


/*
 * when varnoMapping is set it stores an array of varno's in the new query to the original
 * varno's of the source query. This can later be used to understand if the var's used in
 * this query come from an outer rel in a nested loop.
 */
static Query *
GetQueryFromPath(PlannerInfo *root, Path *path, List *tlist, List *clauses,
				 Index **varnoMapping)
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

	if (varnoMapping)
	{
		/* export the reverse varno mapping */
		int mappingSize = list_length(q->rtable);
		*varnoMapping = palloc0(sizeof(Index) * root->simple_rel_array_size);
		for (int i = 0; i < root->simple_rel_array_size; i++)
		{
			Index varno = info.varno_mapping[i];
			if (varno == 0)
			{
				continue;
			}
			(*varnoMapping)[varno] = i;
		}
	}

	return q;
}


void
PathBasedPlannedUpperPathHook(PlannerInfo *root,
							  UpperRelationKind stage,
							  RelOptInfo *input_rel,
							  RelOptInfo *output_rel,
							  void *extra)
{
	if (!UseCustomPath)
	{
		/* path based planner is turned off, don't do anything here */
		return;
	}

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
		{
			PathBasedPlannerGroupAgg(root, input_rel, output_rel, extra);
			return;
		}

		default:
		{
			/* no optimizations implemented, beers for the one that removes this due to being unreachable */
			return;
		}
	}
}


static void
PathBasedPlannerGroupAgg(PlannerInfo *root,
						 RelOptInfo *input_rel,
						 RelOptInfo *output_rel,
						 void *extra)
{
	/*
	 * Here we want to find proof that the group by is right above a distributed union
	 * that is partitioned by the grouping key. If that is the case we can pull the
	 * distributed union above the aggregate which causes it to optimize the plan.
	 *
	 * TODO we just replace the plans for now, but during development we have encountered
	 * a plan that would be better if the grouping would not be pushed down. When the
	 * grouping is solely on a primary key the number of rows will stay the same, while
	 * the width will increase due to any aggregates that could be performed on the data.
	 * This plan has lower network traffic if the grouping would not be pushed down.
	 * Instead of replacing it would benefit the planner to add a new path according to
	 * the potential optimization of pushing down. If <no. rows> * <row width> would be
	 * taken into account in the cost of the plan this would cause magic to happen which
	 * we currently could not support.
	 */

	ListCell *pathCell = NULL;
	foreach(pathCell, output_rel->pathlist)
	{
		Path *originalPath = lfirst(pathCell);
		Path *optimizedGroupAdd = OptimizeGroupAgg(root, originalPath);
		SetListCellPtr(pathCell, optimizedGroupAdd);
	}
}


static Path *
OptimizeGroupAgg(PlannerInfo *root, Path *originalPath)
{
	switch (originalPath->pathtype)
	{
		case T_Agg:
		{
			AggPath *apath = castNode(AggPath, originalPath);
			if (CanOptimizeAggPath(root, apath))
			{
				DistributedUnionPath *distUnion = (DistributedUnionPath *) apath->subpath;
				apath->subpath = distUnion->worker_path;

				/* TODO better cost model, for now substract the DU costs */
				apath->path.startup_cost -= 1000;
				apath->path.total_cost -= 1000;

				return (Path *) WrapTableAccessWithDistributedUnion(
					(Path *) apath,
					distUnion->colocationId,
					distUnion->partitionValue,
					distUnion->sampleRelid,
					distUnion->custom_path.custom_paths);
			}
		}

		default:
		{
			/* no optimisations to be performed*/
			return originalPath;
		}
	}
}


static bool
CanOptimizeAggPath(PlannerInfo *root, AggPath *apath)
{
	if (apath->groupClause == NULL)
	{
		return false;
	}

	if (!IsDistributedUnion(apath->subpath, false, NULL))
	{
		/*
		 * we only can optimize if the path below is a distributed union that we can pull
		 * up, if the path below is not a distributed union we cannot optimize
		 */
		return false;
	}

	SortGroupClause *sgc = NULL;

	/*
	 * TODO verify whats the purpose of the list, if we find any of the distribution
	 * columns somewhere in this we optimize, might be wrong
	 */
	foreach_ptr(sgc, apath->groupClause)
	{
		PathTarget *target = apath->path.pathtarget;
		Expr *targetExpr = NULL;
		Index i = 0;
		foreach_ptr(targetExpr, target->exprs)
		{
			Index targetSortGroupRef = target->sortgrouprefs[i];
			i++;

			if (targetSortGroupRef != sgc->tleSortGroupRef)
			{
				continue;
			}

			if (!IsA(targetExpr, Var))
			{
				continue;
			}

			Var *targetVar = castNode(Var, targetExpr);
			Index rteIndex = targetVar->varno;
			RangeTblEntry *rte = root->simple_rte_array[rteIndex];

			CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(rte->relid);
			if (cacheEntry->partitionColumn == NULL)
			{
				/* a table that is not distributed by a particular column, reference table? */
				continue;
			}

			if (cacheEntry->partitionColumn->varattno == targetVar->varattno)
			{
				/*
				 * grouping column contains the distribution column of a distributed
				 * table, safe to optimize
				 */
				return true;
			}
		}
	}

	return false;
}
