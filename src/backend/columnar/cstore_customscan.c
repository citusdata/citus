/*-------------------------------------------------------------------------
 *
 * cstore_customscan.c
 *
 * This file contains the implementation of a postgres custom scan that
 * we use to push down the projections into the table access methods.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "citus_version.h"
#if USE_TABLEAM

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

#include "columnar/cstore.h"
#include "columnar/cstore_customscan.h"
#include "columnar/cstore_tableam.h"

typedef struct CStoreScanPath
{
	CustomPath custom_path;

	/* place for local state during planning */
} CStoreScanPath;

typedef struct CStoreScanScan
{
	CustomScan custom_scan;

	/* place for local state during execution */
} CStoreScanScan;

typedef struct CStoreScanState
{
	CustomScanState custom_scanstate;

	List *qual;
} CStoreScanState;


static void CStoreSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									 RangeTblEntry *rte);
static Path * CreateCStoreScanPath(RelOptInfo *rel, RangeTblEntry *rte);
static Cost CStoreScanCost(RangeTblEntry *rte);
static Plan * CStoreScanPath_PlanCustomPath(PlannerInfo *root,
											RelOptInfo *rel,
											struct CustomPath *best_path,
											List *tlist,
											List *clauses,
											List *custom_plans);

static Node * CStoreScan_CreateCustomScanState(CustomScan *cscan);

static void CStoreScan_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot * CStoreScan_ExecCustomScan(CustomScanState *node);
static void CStoreScan_EndCustomScan(CustomScanState *node);
static void CStoreScan_ReScanCustomScan(CustomScanState *node);

/* saved hook value in case of unload */
static set_rel_pathlist_hook_type PreviousSetRelPathlistHook = NULL;

static bool EnableCStoreCustomScan = true;


const struct CustomPathMethods CStoreScanPathMethods = {
	.CustomName = "CStoreScan",
	.PlanCustomPath = CStoreScanPath_PlanCustomPath,
};

const struct CustomScanMethods CStoreScanScanMethods = {
	.CustomName = "CStoreScan",
	.CreateCustomScanState = CStoreScan_CreateCustomScanState,
};

const struct CustomExecMethods CStoreExecuteMethods = {
	.CustomName = "CStoreScan",

	.BeginCustomScan = CStoreScan_BeginCustomScan,
	.ExecCustomScan = CStoreScan_ExecCustomScan,
	.EndCustomScan = CStoreScan_EndCustomScan,
	.ReScanCustomScan = CStoreScan_ReScanCustomScan,

	.ExplainCustomScan = NULL,
};


/*
 * cstore_customscan_init installs the hook required to intercept the postgres planner and
 * provide extra paths for cstore tables
 */
void
cstore_customscan_init()
{
	PreviousSetRelPathlistHook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = CStoreSetRelPathlistHook;

	/* register customscan specific GUC's */
	DefineCustomBoolVariable(
		"cstore.enable_custom_scan",
		gettext_noop("Enables the use of a custom scan to push projections and quals "
					 "into the storage layer"),
		NULL,
		&EnableCStoreCustomScan,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL,
		NULL, NULL, NULL);
}


static void
clear_paths(RelOptInfo *rel)
{
	rel->pathlist = NULL;
	rel->partial_pathlist = NULL;
	rel->cheapest_startup_path = NULL;
	rel->cheapest_total_path = NULL;
	rel->cheapest_unique_path = NULL;
}


static void
CStoreSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
						 RangeTblEntry *rte)
{
	/* call into previous hook if assigned */
	if (PreviousSetRelPathlistHook)
	{
		PreviousSetRelPathlistHook(root, rel, rti, rte);
	}

	if (!EnableCStoreCustomScan)
	{
		/* custon scans are disabled, use normal table access method api instead */
		return;
	}

	if (!OidIsValid(rte->relid) || rte->rtekind != RTE_RELATION)
	{
		/* some calls to the pathlist hook don't have a valid relation set. Do nothing */
		return;
	}

	/*
	 * Here we want to inspect if this relation pathlist hook is accessing a cstore table.
	 * If that is the case we want to insert an extra path that pushes down the projection
	 * into the scan of the table to minimize the data read.
	 */
	Relation relation = RelationIdGetRelation(rte->relid);
	if (relation->rd_tableam == GetCstoreTableAmRoutine())
	{
		Path *customPath = CreateCStoreScanPath(rel, rte);

		ereport(DEBUG1, (errmsg("pathlist hook for cstore table am")));

		/* we propose a new path that will be the only path for scanning this relation */
		clear_paths(rel);
		add_path(rel, customPath);
	}
	RelationClose(relation);
}


static Path *
CreateCStoreScanPath(RelOptInfo *rel, RangeTblEntry *rte)
{
	CStoreScanPath *cspath = (CStoreScanPath *) newNode(sizeof(CStoreScanPath),
														T_CustomPath);

	/*
	 * popuate custom path information
	 */
	CustomPath *cpath = &cspath->custom_path;
	cpath->methods = &CStoreScanPathMethods;

	/*
	 * populate generic path information
	 */
	Path *path = &cpath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;

	/*
	 * Add cost estimates for a cstore table scan, row count is the rows estimated by
	 * postgres' planner.
	 */
	path->rows = rel->rows;
	path->startup_cost = 0;
	path->total_cost = path->startup_cost + CStoreScanCost(rte);

	return (Path *) cspath;
}


/*
 * CStoreScanCost calculates the cost of scanning the cstore table. The cost is estimated
 * by using all stripe metadata to estimate based on the columns to read how many pages
 * need to be read.
 */
static Cost
CStoreScanCost(RangeTblEntry *rte)
{
	Relation rel = RelationIdGetRelation(rte->relid);
	DataFileMetadata *metadata = ReadDataFileMetadata(rel->rd_node.relNode, false);
	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	ListCell *stripeMetadataCell = NULL;

	RelationClose(rel);
	rel = NULL;

	foreach(stripeMetadataCell, metadata->stripeMetadataList)
	{
		StripeMetadata *stripeMetadata = (StripeMetadata *) lfirst(stripeMetadataCell);
		totalStripeSize += stripeMetadata->dataLength;
		maxColumnCount = Max(maxColumnCount, stripeMetadata->columnCount);
	}

	{
		Bitmapset *attr_needed = rte->selectedCols;
		double numberOfColumnsRead = bms_num_members(attr_needed);
		double selectionRatio = numberOfColumnsRead / (double) maxColumnCount;
		Cost scanCost = (double) totalStripeSize / BLCKSZ * selectionRatio;
		return scanCost;
	}
}


static Plan *
CStoreScanPath_PlanCustomPath(PlannerInfo *root,
							  RelOptInfo *rel,
							  struct CustomPath *best_path,
							  List *tlist,
							  List *clauses,
							  List *custom_plans)
{
	CStoreScanScan *plan = (CStoreScanScan *) newNode(sizeof(CStoreScanScan),
													  T_CustomScan);

	CustomScan *cscan = &plan->custom_scan;
	cscan->methods = &CStoreScanScanMethods;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	clauses = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.plan.qual = clauses;
	cscan->scan.scanrelid = best_path->path.parent->relid;

	return (Plan *) plan;
}


static Node *
CStoreScan_CreateCustomScanState(CustomScan *cscan)
{
	CStoreScanState *cstorescanstate = (CStoreScanState *) newNode(
		sizeof(CStoreScanState), T_CustomScanState);

	CustomScanState *cscanstate = &cstorescanstate->custom_scanstate;
	cscanstate->methods = &CStoreExecuteMethods;

	cstorescanstate->qual = cscan->scan.plan.qual;

	return (Node *) cscanstate;
}


static void
CStoreScan_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags)
{
	/* scan slot is already initialized */
}


static Bitmapset *
CStoreAttrNeeded(ScanState *ss)
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
CStoreScanNext(CStoreScanState *cstorescanstate)
{
	CustomScanState *node = (CustomScanState *) cstorescanstate;

	/*
	 * get information from the estate and scan state
	 */
	TableScanDesc scandesc = node->ss.ss_currentScanDesc;
	EState *estate = node->ss.ps.state;
	ScanDirection direction = estate->es_direction;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/* the cstore access method does not use the flags, they are specific to heap */
		uint32 flags = 0;
		Bitmapset *attr_needed = CStoreAttrNeeded(&node->ss);

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = cstore_beginscan_extended(node->ss.ss_currentRelation,
											 estate->es_snapshot,
											 0, NULL, NULL, flags, attr_needed,
											 cstorescanstate->qual);
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
CStoreScanRecheck(CStoreScanState *node, TupleTableSlot *slot)
{
	return true;
}


static TupleTableSlot *
CStoreScan_ExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) CStoreScanNext,
					(ExecScanRecheckMtd) CStoreScanRecheck);
}


static void
CStoreScan_EndCustomScan(CustomScanState *node)
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
CStoreScan_ReScanCustomScan(CustomScanState *node)
{
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;
	if (scanDesc != NULL)
	{
		table_rescan(node->ss.ss_currentScanDesc, NULL);
	}
}


#endif /* USE_TABLEAM */
