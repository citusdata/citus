/*-------------------------------------------------------------------------
 *
 * columnar_customscan.c
 *
 * This file contains the implementation of a postgres custom scan that
 * we use to push down the projections into the table access methods.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "citus_version.h"

#include "postgres.h"

#include "access/skey.h"
#include "nodes/extensible.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "utils/relcache.h"

#include "columnar/columnar_customscan.h"
#include "columnar/columnar_metadata.h"
#include "columnar/columnar_tableam.h"
#include "distributed/listutils.h"

typedef struct ColumnarScanPath
{
	CustomPath custom_path;

	/* place for local state during planning */
} ColumnarScanPath;

typedef struct ColumnarScanScan
{
	CustomScan custom_scan;

	/* place for local state during execution */
} ColumnarScanScan;

typedef struct ColumnarScanState
{
	CustomScanState custom_scanstate;

	List *qual;
} ColumnarScanState;


typedef bool (*PathPredicate)(Node *node);


static void ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);
static void RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate);
static bool IsNotIndexPath(Node *node);
static Path * CreateColumnarScanPath(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte);
static Cost ColumnarScanCost(RangeTblEntry *rte);
static Plan * ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
											  RelOptInfo *rel,
											  struct CustomPath *best_path,
											  List *tlist,
											  List *clauses,
											  List *custom_plans);

static Node * ColumnarScan_CreateCustomScanState(CustomScan *cscan);

static void ColumnarScan_BeginCustomScan(CustomScanState *node, EState *estate, int
										 eflags);
static TupleTableSlot * ColumnarScan_ExecCustomScan(CustomScanState *node);
static void ColumnarScan_EndCustomScan(CustomScanState *node);
static void ColumnarScan_ReScanCustomScan(CustomScanState *node);
static void ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
										   ExplainState *es);

/* saved hook value in case of unload */
static set_rel_pathlist_hook_type PreviousSetRelPathlistHook = NULL;

static bool EnableColumnarCustomScan = true;
static bool EnableColumnarQualPushdown = true;


const struct CustomPathMethods ColumnarScanPathMethods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = ColumnarScanPath_PlanCustomPath,
};

const struct CustomScanMethods ColumnarScanScanMethods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = ColumnarScan_CreateCustomScanState,
};

const struct CustomExecMethods ColumnarExecuteMethods = {
	.CustomName = "ColumnarScan",

	.BeginCustomScan = ColumnarScan_BeginCustomScan,
	.ExecCustomScan = ColumnarScan_ExecCustomScan,
	.EndCustomScan = ColumnarScan_EndCustomScan,
	.ReScanCustomScan = ColumnarScan_ReScanCustomScan,

	.ExplainCustomScan = ColumnarScan_ExplainCustomScan,
};


/*
 * columnar_customscan_init installs the hook required to intercept the postgres planner and
 * provide extra paths for columnar tables
 */
void
columnar_customscan_init()
{
	PreviousSetRelPathlistHook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ColumnarSetRelPathlistHook;

	/* register customscan specific GUC's */
	DefineCustomBoolVariable(
		"columnar.enable_custom_scan",
		gettext_noop("Enables the use of a custom scan to push projections and quals "
					 "into the storage layer."),
		NULL,
		&EnableColumnarCustomScan,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"columnar.enable_qual_pushdown",
		gettext_noop("Enables qual pushdown into columnar. This has no effect unless "
					 "columnar.enable_custom_scan is true."),
		NULL,
		&EnableColumnarQualPushdown,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);

	RegisterCustomScanMethods(&ColumnarScanScanMethods);
}


static void
ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
						   RangeTblEntry *rte)
{
	/* call into previous hook if assigned */
	if (PreviousSetRelPathlistHook)
	{
		PreviousSetRelPathlistHook(root, rel, rti, rte);
	}

	if (!OidIsValid(rte->relid) || rte->rtekind != RTE_RELATION || rte->inh)
	{
		/* some calls to the pathlist hook don't have a valid relation set. Do nothing */
		return;
	}

	/*
	 * Here we want to inspect if this relation pathlist hook is accessing a columnar table.
	 * If that is the case we want to insert an extra path that pushes down the projection
	 * into the scan of the table to minimize the data read.
	 */
	Relation relation = RelationIdGetRelation(rte->relid);
	if (relation->rd_tableam == GetColumnarTableAmRoutine())
	{
		if (rte->tablesample != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("sample scans not supported on columnar tables")));
		}

		/* columnar doesn't support parallel paths */
		rel->partial_pathlist = NIL;

		if (EnableColumnarCustomScan)
		{
			Path *customPath = CreateColumnarScanPath(root, rel, rte);

			ereport(DEBUG1, (errmsg("pathlist hook for columnar table am")));

			/*
			 * TODO: Since we don't have a proper costing model for
			 * ColumnarCustomScan, we remove other paths to force postgres
			 * using ColumnarCustomScan. Note that we still keep index paths
			 * since they still might be useful.
			 */
			RemovePathsByPredicate(rel, IsNotIndexPath);
			add_path(rel, customPath);
		}
	}
	RelationClose(relation);
}


/*
 * RemovePathsByPredicate removes the paths that removePathPredicate
 * evaluates to true from pathlist of given rel.
 */
static void
RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate)
{
	List *filteredPathList = NIL;

	Node *node = NULL;
	foreach_ptr(node, rel->pathlist)
	{
		if (!removePathPredicate(node))
		{
			filteredPathList = lappend(filteredPathList, node);
		}
	}

	rel->pathlist = filteredPathList;
}


/*
 * IsNotIndexPath returns true if given node is not an IndexPath.
 */
static bool
IsNotIndexPath(Node *node)
{
	return !IsA(node, IndexPath);
}


static Path *
CreateColumnarScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	ColumnarScanPath *cspath = (ColumnarScanPath *) newNode(sizeof(ColumnarScanPath),
															T_CustomPath);

	/*
	 * popuate custom path information
	 */
	CustomPath *cpath = &cspath->custom_path;
	cpath->methods = &ColumnarScanPathMethods;

	/*
	 * populate generic path information
	 */
	Path *path = &cpath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;

	/* columnar scans are not parallel-aware, but they are parallel-safe */
	path->parallel_safe = rel->consider_parallel;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	path->param_info = get_baserel_parampathinfo(root, rel,
												 rel->lateral_relids);

	/*
	 * Add cost estimates for a columnar table scan, row count is the rows estimated by
	 * postgres' planner.
	 */
	path->rows = rel->rows;
	path->startup_cost = 0;
	path->total_cost = path->startup_cost + ColumnarScanCost(rte);

	return (Path *) cspath;
}


/*
 * ColumnarScanCost calculates the cost of scanning the columnar table. The cost is estimated
 * by using all stripe metadata to estimate based on the columns to read how many pages
 * need to be read.
 */
static Cost
ColumnarScanCost(RangeTblEntry *rte)
{
	Relation rel = RelationIdGetRelation(rte->relid);
	List *stripeList = StripesForRelfilenode(rel->rd_node);
	RelationClose(rel);

	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	ListCell *stripeMetadataCell = NULL;
	rel = NULL;

	foreach(stripeMetadataCell, stripeList)
	{
		StripeMetadata *stripeMetadata = (StripeMetadata *) lfirst(stripeMetadataCell);
		totalStripeSize += stripeMetadata->dataLength;
		maxColumnCount = Max(maxColumnCount, stripeMetadata->columnCount);
	}

	{
		Bitmapset *attr_needed = rte->selectedCols;
		double numberOfColumnsRead = bms_num_members(attr_needed);
		double selectionRatio = 0;

		/*
		 * When no stripes are in the table we don't have a count in maxColumnCount. To
		 * prevent a division by zero turning into a NaN we keep the ratio on zero.
		 * This will result in a cost of 0 for scanning the table which is a reasonable
		 * cost on an empty table.
		 */
		if (maxColumnCount != 0)
		{
			selectionRatio = numberOfColumnsRead / (double) maxColumnCount;
		}
		Cost scanCost = (double) totalStripeSize / BLCKSZ * selectionRatio;
		return scanCost;
	}
}


static Plan *
ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
								RelOptInfo *rel,
								struct CustomPath *best_path,
								List *tlist,
								List *clauses,
								List *custom_plans)
{
	ColumnarScanScan *plan = (ColumnarScanScan *) newNode(sizeof(ColumnarScanScan),
														  T_CustomScan);

	CustomScan *cscan = &plan->custom_scan;
	cscan->methods = &ColumnarScanScanMethods;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.plan.qual = clauses;
	cscan->scan.scanrelid = best_path->path.parent->relid;

	return (Plan *) plan;
}


static Node *
ColumnarScan_CreateCustomScanState(CustomScan *cscan)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) newNode(
		sizeof(ColumnarScanState), T_CustomScanState);

	CustomScanState *cscanstate = &columnarScanState->custom_scanstate;
	cscanstate->methods = &ColumnarExecuteMethods;

	if (EnableColumnarQualPushdown)
	{
		columnarScanState->qual = cscan->scan.plan.qual;
	}

	return (Node *) cscanstate;
}


static void
ColumnarScan_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags)
{
	/* scan slot is already initialized */
}


static Bitmapset *
ColumnarAttrNeeded(ScanState *ss)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	Bitmapset *attr_needed = NULL;
	Plan *plan = ss->ps.plan;
	int flags = PVC_RECURSE_AGGREGATES |
				PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *) plan->targetlist, flags),
							 pull_var_clause((Node *) plan->qual, flags));
	ListCell *lc;

	foreach(lc, vars)
	{
		Var *var = lfirst(lc);

		if (var->varattno < 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"UPDATE and CTID scans not supported for ColumnarScan")));
		}

		if (var->varattno == 0)
		{
			elog(DEBUG1, "Need attribute: all");

			/* all attributes are required, we don't need to add more so break*/
			attr_needed = bms_add_range(attr_needed, 0, natts - 1);
			break;
		}

		elog(DEBUG1, "Need attribute: %d", var->varattno);
		attr_needed = bms_add_member(attr_needed, var->varattno - 1);
	}

	return attr_needed;
}


static TupleTableSlot *
ColumnarScanNext(ColumnarScanState *columnarScanState)
{
	CustomScanState *node = (CustomScanState *) columnarScanState;

	/*
	 * get information from the estate and scan state
	 */
	TableScanDesc scandesc = node->ss.ss_currentScanDesc;
	EState *estate = node->ss.ps.state;
	ScanDirection direction = estate->es_direction;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/* the columnar access method does not use the flags, they are specific to heap */
		uint32 flags = 0;
		Bitmapset *attr_needed = ColumnarAttrNeeded(&node->ss);

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = columnar_beginscan_extended(node->ss.ss_currentRelation,
											   estate->es_snapshot,
											   0, NULL, NULL, flags, attr_needed,
											   columnarScanState->qual);
		bms_free(attr_needed);

		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
	{
		return slot;
	}
	return NULL;
}


/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
ColumnarScanRecheck(ColumnarScanState *node, TupleTableSlot *slot)
{
	return true;
}


static TupleTableSlot *
ColumnarScan_ExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) ColumnarScanNext,
					(ExecScanRecheckMtd) ColumnarScanRecheck);
}


static void
ColumnarScan_EndCustomScan(CustomScanState *node)
{
	/*
	 * get information from node
	 */
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
	{
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
	{
		table_endscan(scanDesc);
	}
}


static void
ColumnarScan_ReScanCustomScan(CustomScanState *node)
{
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;
	if (scanDesc != NULL)
	{
		table_rescan(node->ss.ss_currentScanDesc, NULL);
	}
}


static void
ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
							   ExplainState *es)
{
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	if (scanDesc != NULL)
	{
		int64 chunkGroupsFiltered = ColumnarScanChunkGroupsFiltered(scanDesc);
		ExplainPropertyInteger("Columnar Chunk Groups Removed by Filter", NULL,
							   chunkGroupsFiltered, es);
	}
}
