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

#include "access/amapi.h"
#include "access/skey.h"
#include "nodes/extensible.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "utils/relcache.h"
#include "utils/spccache.h"

#include "columnar/columnar.h"
#include "columnar/columnar_customscan.h"
#include "columnar/columnar_metadata.h"
#include "columnar/columnar_tableam.h"
#include "distributed/listutils.h"

/*
 * ColumnarScanState represents the state for a columnar scan. It's a
 * CustomScanState with additional fields specific to columnar scans.
 */
typedef struct ColumnarScanState
{
	CustomScanState custom_scanstate; /* must be first field */

	List *qual;
} ColumnarScanState;


typedef bool (*PathPredicate)(Path *path);


static void ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);
static void RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate);
static bool IsNotIndexPath(Path *path);
static Path * CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel,
										Oid relationId);
static void RecostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId);
static void RecostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
									IndexPath *indexPath);
static Cost ColumnarIndexScanAddStartupCost(RelOptInfo *rel, Oid relationId,
											IndexPath *indexPath);
static Cost ColumnarIndexScanAddTotalCost(PlannerInfo *root, RelOptInfo *rel,
										  Oid relationId, IndexPath *indexPath);
static void RecostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path);
static int RelationIdGetNumberOfAttributes(Oid relationId);
static Path * CreateColumnarScanPath(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte);
static Cost ColumnarScanCost(RelOptInfo *rel, Oid relationId, int numberOfColumnsRead);
static Cost ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId,
									  int numberOfColumnsRead);
static uint64 ColumnarTableStripeCount(Oid relationId);
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

const struct CustomExecMethods ColumnarScanExecuteMethods = {
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

		/*
		 * There are cases where IndexPath is normally more preferrable over
		 * SeqPath for heapAM but not for columnarAM. In such cases, an
		 * IndexPath could wrongly dominate a SeqPath based on the costs
		 * estimated by postgres earlier. For this reason, here we manually
		 * create a SeqPath, estimate the cost based on columnarAM and append
		 * to pathlist.
		 *
		 * Before doing that, we first re-cost all the existing paths so that
		 * add_path makes correct cost comparisons when appending our SeqPath.
		 */
		RecostColumnarPaths(root, rel, rte->relid);

		Path *seqPath = CreateColumnarSeqScanPath(root, rel, rte->relid);
		add_path(rel, seqPath);

		if (EnableColumnarCustomScan)
		{
			Path *customPath = CreateColumnarScanPath(root, rel, rte);

			ereport(DEBUG1, (errmsg("pathlist hook for columnar table am")));

			/*
			 * When columnar custom scan is enabled (columnar.enable_custom_scan),
			 * we only consider ColumnarScanPath's & IndexPath's. For this reason,
			 * we remove other paths and re-estimate IndexPath costs to make accurate
			 * comparisons between them.
			 *
			 * Even more, we might calculate an equal cost for a
			 * ColumnarCustomScan and a SeqPath if we are reading all columns
			 * of given table since we don't consider chunk group filtering
			 * when costing ColumnarCustomScan.
			 * In that case, if we don't remove SeqPath's, we might wrongly choose
			 * SeqPath thinking that its cost would be equal to ColumnarCustomScan.
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

	Path *path = NULL;
	foreach_ptr(path, rel->pathlist)
	{
		if (!removePathPredicate(path))
		{
			filteredPathList = lappend(filteredPathList, path);
		}
	}

	rel->pathlist = filteredPathList;
}


/*
 * IsNotIndexPath returns true if given path is not an IndexPath.
 */
static bool
IsNotIndexPath(Path *path)
{
	return !IsA(path, IndexPath);
}


/*
 * CreateColumnarSeqScanPath returns Path for sequential scan on columnar
 * table with relationId.
 */
static Path *
CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	/* columnar doesn't support parallel scan */
	int parallelWorkers = 0;

	Relids requiredOuter = rel->lateral_relids;
	Path *path = create_seqscan_path(root, rel, requiredOuter, parallelWorkers);
	RecostColumnarSeqPath(rel, relationId, path);
	return path;
}


/*
 * RecostColumnarPaths re-costs paths of given RelOptInfo for
 * columnar table with relationId.
 */
static void
RecostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	Path *path = NULL;
	foreach_ptr(path, rel->pathlist)
	{
		if (IsA(path, IndexPath))
		{
			/*
			 * Since we don't provide implementations for scan_bitmap_next_block
			 * & scan_bitmap_next_tuple, postgres doesn't generate bitmap index
			 * scan paths for columnar tables already (see related comments in
			 * TableAmRoutine). For this reason, we only consider IndexPath's
			 * here.
			 */
			RecostColumnarIndexPath(root, rel, relationId, (IndexPath *) path);
		}
		else if (path->pathtype == T_SeqScan)
		{
			RecostColumnarSeqPath(rel, relationId, path);
		}
	}
}


/*
 * RecostColumnarIndexPath re-costs given index path for columnar table with
 * relationId.
 */
static void
RecostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
						IndexPath *indexPath)
{
	if (!enable_indexscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	ereport(DEBUG4, (errmsg("columnar table index scan costs estimated by "
							"indexAM: startup cost = %.10f, total cost = "
							"%.10f", indexPath->path.startup_cost,
							indexPath->path.total_cost)));

	/*
	 * We estimate the cost for columnar table read during index scan. Also,
	 * instead of overwriting startup & total costs, we "add" ours to the
	 * costs estimated by indexAM since we should consider index traversal
	 * related costs too.
	 */
	Cost indexAMStartupCost = indexPath->path.startup_cost;
	Cost indexAMScanCost = indexPath->path.total_cost - indexAMStartupCost;

	Cost columnarIndexScanStartupCost = ColumnarIndexScanAddStartupCost(rel, relationId,
																		indexPath);
	Cost columnarIndexScanCost = ColumnarIndexScanAddTotalCost(root, rel, relationId,
															   indexPath);

	indexPath->path.startup_cost = indexAMStartupCost + columnarIndexScanStartupCost;
	indexPath->path.total_cost = indexPath->path.startup_cost +
								 indexAMScanCost + columnarIndexScanCost;

	ereport(DEBUG4, (errmsg("columnar table index scan costs re-estimated "
							"by columnarAM (including indexAM costs): "
							"startup cost = %.10f, total cost = %.10f",
							indexPath->path.startup_cost,
							indexPath->path.total_cost)));
}


/*
 * ColumnarIndexScanAddStartupCost returns additional startup cost estimated
 * for index scan described by IndexPath for columnar table with relationId.
 */
static Cost
ColumnarIndexScanAddStartupCost(RelOptInfo *rel, Oid relationId, IndexPath *indexPath)
{
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);

	/* we would at least read one stripe */
	return ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * ColumnarIndexScanAddTotalCost returns additional cost estimated for
 * index scan described by IndexPath for columnar table with relationId.
 */
static Cost
ColumnarIndexScanAddTotalCost(PlannerInfo *root, RelOptInfo *rel,
							  Oid relationId, IndexPath *indexPath)
{
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);
	Cost perStripeCost = ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);

	/*
	 * We don't need to pass correct loop count to amcostestimate since we
	 * will only use index correlation & index selectivity, and loop count
	 * doesn't have any effect on those two.
	 */
	double fakeLoopCount = 1;
	Cost fakeIndexStartupCost;
	Cost fakeIndexTotalCost;
	double fakeIndexPages;
	Selectivity indexSelectivity;
	double indexCorrelation;
	amcostestimate_function amcostestimate = indexPath->indexinfo->amcostestimate;
	amcostestimate(root, indexPath, fakeLoopCount, &fakeIndexStartupCost,
				   &fakeIndexTotalCost, &indexSelectivity,
				   &indexCorrelation, &fakeIndexPages);

	Relation relation = RelationIdGetRelation(relationId);
	uint64 rowCount = ColumnarTableRowCount(relation);
	RelationClose(relation);
	double estimatedRows = rowCount * indexSelectivity;

	/*
	 * In the worst case (i.e no correlation between the column & the index),
	 * we need to read a different stripe for each row.
	 */
	double maxStripeReadCount = estimatedRows;

	/*
	 * In the best case (i.e the column is fully correlated with the index),
	 * we wouldn't read the same stripe again and again thanks
	 * to locality.
	 */
	double avgStripeRowCount =
		rowCount / (double) ColumnarTableStripeCount(relationId);
	double minStripeReadCount = estimatedRows / avgStripeRowCount;

	/*
	 * While being close to 0 means low correlation, being close to -1 or +1
	 * means high correlation. For index scans on columnar tables, it doesn't
	 * matter if the column and the index are "correlated" (+1) or
	 * "anti-correlated" (-1) since both help us avoiding from reading the
	 * same stripe again and again.
	 */
	double absIndexCorrelation = Abs(indexCorrelation);

	/*
	 * To estimate the number of stripes that we need to read, we do linear
	 * interpolation between minStripeReadCount & maxStripeReadCount. To do
	 * that, we use complement to 1 of absolute correlation, where being
	 * close to 0 means high correlation and being close to 1 means low
	 * correlation.
	 * In practice, we only want to do an index scan when absIndexCorrelation
	 * is 1 (or extremely close to it), or when the absolute number of tuples
	 * returned is very small. Other cases will have a prohibitive cost.
	 */
	double complementIndexCorrelation = 1 - absIndexCorrelation;
	double estimatedStripeReadCount =
		minStripeReadCount + complementIndexCorrelation * (maxStripeReadCount -
														   minStripeReadCount);

	Cost scanCost = perStripeCost * estimatedStripeReadCount;

	ereport(DEBUG4, (errmsg("re-costing index scan for columnar table: "
							"selectivity = %.10f, complement abs "
							"correlation = %.10f, per stripe cost = %.10f, "
							"estimated stripe read count = %.10f, "
							"total additional cost = %.10f",
							indexSelectivity, complementIndexCorrelation,
							perStripeCost, estimatedStripeReadCount,
							scanCost)));

	return scanCost;
}


/*
 * RecostColumnarSeqPath re-costs given seq path for columnar table with
 * relationId.
 */
static void
RecostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path)
{
	if (!enable_seqscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	path->startup_cost = 0;

	/*
	 * Seq scan doesn't support projection pushdown, so we will read all the
	 * columns.
	 * Also note that seq scan doesn't support chunk group filtering too but
	 * our costing model already doesn't consider chunk group filtering.
	 */
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);
	path->total_cost = path->startup_cost +
					   ColumnarScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * RelationIdGetNumberOfAttributes returns number of attributes that relation
 * with relationId has.
 */
static int
RelationIdGetNumberOfAttributes(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	int nattrs = relation->rd_att->natts;
	RelationClose(relation);
	return nattrs;
}


static Path *
CreateColumnarScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Must return a CustomPath, not a larger structure containing a
	 * CustomPath as the first field. Otherwise, nodeToString() will fail to
	 * output the additional fields.
	 */
	CustomPath *cpath = (CustomPath *) newNode(sizeof(CustomPath), T_CustomPath);

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
	int numberOfColumnsRead = bms_num_members(rte->selectedCols);
	path->total_cost = path->startup_cost +
					   ColumnarScanCost(rel, rte->relid, numberOfColumnsRead);

	return (Path *) cpath;
}


/*
 * ColumnarScanCost calculates the cost of scanning the columnar table. The cost is estimated
 * by using all stripe metadata to estimate based on the columns to read how many pages
 * need to be read.
 */
static Cost
ColumnarScanCost(RelOptInfo *rel, Oid relationId, int numberOfColumnsRead)
{
	return ColumnarTableStripeCount(relationId) *
		   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * ColumnarPerStripeScanCost calculates the cost to scan a single stripe
 * of given columnar table based on number of columns that needs to be
 * read during scan operation.
 */
static Cost
ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId, int numberOfColumnsRead)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(relation->rd_node);
	RelationClose(relation);

	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	StripeMetadata *stripeMetadata = NULL;
	foreach_ptr(stripeMetadata, stripeList)
	{
		totalStripeSize += stripeMetadata->dataLength;
		maxColumnCount = Max(maxColumnCount, stripeMetadata->columnCount);
	}

	/*
	 * When no stripes are in the table we don't have a count in maxColumnCount. To
	 * prevent a division by zero turning into a NaN we keep the ratio on zero.
	 * This will result in a cost of 0 for scanning the table which is a reasonable
	 * cost on an empty table.
	 */
	if (maxColumnCount == 0)
	{
		return 0;
	}

	double columnSelectionRatio = numberOfColumnsRead / (double) maxColumnCount;
	Cost tableScanCost = (double) totalStripeSize / BLCKSZ * columnSelectionRatio;
	Cost perStripeScanCost = tableScanCost / list_length(stripeList);

	/*
	 * Finally, multiply the cost of reading a single stripe by seq page read
	 * cost to make our estimation scale compatible with postgres.
	 * Since we are calculating the cost for a single stripe here, we use seq
	 * page cost instead of random page cost. This is because, random page
	 * access only happens when switching between columns, which is pretty
	 * much neglactable.
	 */
	double relSpaceSeqPageCost;
	get_tablespace_page_costs(rel->reltablespace,
							  NULL, &relSpaceSeqPageCost);
	perStripeScanCost = perStripeScanCost * relSpaceSeqPageCost;

	return perStripeScanCost;
}


/*
 * ColumnarTableStripeCount returns the number of stripes that columnar
 * table with relationId has by using stripe metadata.
 */
static uint64
ColumnarTableStripeCount(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(relation->rd_node);
	int stripeCount = list_length(stripeList);
	RelationClose(relation);

	return stripeCount;
}


static Plan *
ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
								RelOptInfo *rel,
								struct CustomPath *best_path,
								List *tlist,
								List *clauses,
								List *custom_plans)
{
	/*
	 * Must return a CustomScan, not a larger structure containing a
	 * CustomScan as the first field. Otherwise, copyObject() will fail to
	 * copy the additional fields.
	 */
	CustomScan *cscan = (CustomScan *) newNode(sizeof(CustomScan), T_CustomScan);

	cscan->methods = &ColumnarScanScanMethods;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.plan.qual = clauses;
	cscan->scan.scanrelid = best_path->path.parent->relid;

	return (Plan *) cscan;
}


static Node *
ColumnarScan_CreateCustomScanState(CustomScan *cscan)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) newNode(
		sizeof(ColumnarScanState), T_CustomScanState);

	CustomScanState *cscanstate = &columnarScanState->custom_scanstate;
	cscanstate->methods = &ColumnarScanExecuteMethods;

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
