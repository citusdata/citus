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

#include <math.h>

#include "postgres.h"

#include "miscadmin.h"

#include "access/amapi.h"
#include "access/skey.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "commands/defrem.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"

#include "citus_version.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#endif
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
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

	ExprContext *css_RuntimeContext;
	List *qual;
} ColumnarScanState;


typedef bool (*PathPredicate)(Path *path);


/* functions to cost paths in-place */
static void CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId);
static void CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
								  IndexPath *indexPath);
static void CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path);
static void CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
							 CustomPath *cpath, int numberOfColumnsRead,
							 int nClauses);

/* functions to add new paths */
static void AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel,
								 RangeTblEntry *rte);
static void AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte, Relids required_relids);

/* helper functions to be used when costing paths or altering them */
static void RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate);
static bool IsNotIndexPath(Path *path);
static Cost ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
											Oid relationId, IndexPath *indexPath);
static int RelationIdGetNumberOfAttributes(Oid relationId);
static Cost ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId,
									  int numberOfColumnsRead);
static uint64 ColumnarTableStripeCount(Oid relationId);
static Path * CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel,
										Oid relationId);
static void AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel,
									RangeTblEntry *rte, Relids paramRelids,
									Relids candidateRelids,
									int depthLimit);

/* hooks and callbacks */
static void ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);
static void ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
										bool inhparent, RelOptInfo *rel);
static Plan * ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
											  RelOptInfo *rel,
											  struct CustomPath *best_path,
											  List *tlist,
											  List *clauses,
											  List *custom_plans);
static List * ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
															   List *custom_private,
															   RelOptInfo *child_rel);
static Node * ColumnarScan_CreateCustomScanState(CustomScan *cscan);

static void ColumnarScan_BeginCustomScan(CustomScanState *node, EState *estate,
										 int eflags);
static TupleTableSlot * ColumnarScan_ExecCustomScan(CustomScanState *node);
static void ColumnarScan_EndCustomScan(CustomScanState *node);
static void ColumnarScan_ReScanCustomScan(CustomScanState *node);
static void ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
										   ExplainState *es);

/* helper functions to build strings for EXPLAIN */
static const char * ColumnarPushdownClausesStr(List *context, List *clauses);
static const char * ColumnarProjectedColumnsStr(List *context,
												List *projectedColumns);
static List * set_deparse_context_planstate(List *dpcontext, Node *node,
											List *ancestors);

/* other helpers */
static List * ColumnarVarNeeded(ColumnarScanState *columnarScanState);
static Bitmapset * ColumnarAttrNeeded(ScanState *ss);
#if PG_VERSION_NUM >= PG_VERSION_16
static Bitmapset * fixup_inherited_columns(Oid parentId, Oid childId, Bitmapset *columns);
#endif

/* saved hook value in case of unload */
static set_rel_pathlist_hook_type PreviousSetRelPathlistHook = NULL;
static get_relation_info_hook_type PreviousGetRelationInfoHook = NULL;

static bool EnableColumnarCustomScan = true;
static bool EnableColumnarQualPushdown = true;
static double ColumnarQualPushdownCorrelationThreshold = 0.9;
static int ColumnarMaxCustomScanPaths = 64;
static int ColumnarPlannerDebugLevel = DEBUG3;


const struct CustomPathMethods ColumnarScanPathMethods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = ColumnarScanPath_PlanCustomPath,
	.ReparameterizeCustomPathByChild = ColumnarScanPath_ReparameterizeCustomPathByChild,
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

static const struct config_enum_entry debug_level_options[] = {
	{ "debug5", DEBUG5, false },
	{ "debug4", DEBUG4, false },
	{ "debug3", DEBUG3, false },
	{ "debug2", DEBUG2, false },
	{ "debug1", DEBUG1, false },
	{ "debug", DEBUG2, true },
	{ "info", INFO, false },
	{ "notice", NOTICE, false },
	{ "warning", WARNING, false },
	{ "log", LOG, false },
	{ NULL, 0, false }
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

	PreviousGetRelationInfoHook = get_relation_info_hook;
	get_relation_info_hook = ColumnarGetRelationInfoHook;

	/* register customscan specific GUC's */
	DefineCustomBoolVariable(
		"columnar.enable_custom_scan",
		gettext_noop("Enables the use of a custom scan to push projections and quals "
					 "into the storage layer."),
		NULL,
		&EnableColumnarCustomScan,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"columnar.enable_qual_pushdown",
		gettext_noop("Enables qual pushdown into columnar. This has no effect unless "
					 "columnar.enable_custom_scan is true."),
		NULL,
		&EnableColumnarQualPushdown,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomRealVariable(
		"columnar.qual_pushdown_correlation_threshold",
		gettext_noop("Correlation threshold to attempt to push a qual "
					 "referencing the given column. A value of 0 means "
					 "attempt to push down all quals, even if the column "
					 "is uncorrelated."),
		NULL,
		&ColumnarQualPushdownCorrelationThreshold,
		0.9,
		0.0,
		1.0,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomIntVariable(
		"columnar.max_custom_scan_paths",
		gettext_noop("Maximum number of custom scan paths to generate "
					 "for a columnar table when planning."),
		NULL,
		&ColumnarMaxCustomScanPaths,
		64,
		1,
		1024,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomEnumVariable(
		"columnar.planner_debug_level",
		"Message level for columnar planning information.",
		NULL,
		&ColumnarPlannerDebugLevel,
		DEBUG3,
		debug_level_options,
		PGC_USERSET,
		0,
		NULL,
		NULL,
		NULL);

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
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", rte->relid)));
	}

	if (relation->rd_tableam == GetColumnarTableAmRoutine())
	{
		if (rte->tablesample != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("sample scans not supported on columnar tables")));
		}

		if (list_length(rel->partial_pathlist) != 0)
		{
			/*
			 * Parallel scans on columnar tables are already discardad by
			 * ColumnarGetRelationInfoHook but be on the safe side.
			 */
			elog(ERROR, "parallel scans on columnar are not supported");
		}

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
		CostColumnarPaths(root, rel, rte->relid);

		Path *seqPath = CreateColumnarSeqScanPath(root, rel, rte->relid);
		add_path(rel, seqPath);

		if (EnableColumnarCustomScan)
		{
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
			AddColumnarScanPaths(root, rel, rte);
		}
	}
	RelationClose(relation);
}


static void
ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
							bool inhparent, RelOptInfo *rel)
{
	if (PreviousGetRelationInfoHook)
	{
		PreviousGetRelationInfoHook(root, relationObjectId, inhparent, rel);
	}

	if (IsColumnarTableAmTable(relationObjectId))
	{
		/* disable parallel query */
		rel->rel_parallel_workers = 0;

		/* disable index-only scan */
		IndexOptInfo *indexOptInfo = NULL;
		foreach_declared_ptr(indexOptInfo, rel->indexlist)
		{
			memset(indexOptInfo->canreturn, false, indexOptInfo->ncolumns * sizeof(bool));
		}
	}
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
	foreach_declared_ptr(path, rel->pathlist)
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
	CostColumnarSeqPath(rel, relationId, path);
	return path;
}


/*
 * CostColumnarPaths re-costs paths of given RelOptInfo for
 * columnar table with relationId.
 */
static void
CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	Path *path = NULL;
	foreach_declared_ptr(path, rel->pathlist)
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
			CostColumnarIndexPath(root, rel, relationId, (IndexPath *) path);
		}
		else if (path->pathtype == T_SeqScan)
		{
			CostColumnarSeqPath(rel, relationId, path);
		}
	}
}


/*
 * CostColumnarIndexPath re-costs given index path for columnar table with
 * relationId.
 */
static void
CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
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
	 * instead of overwriting total cost, we "add" ours to the cost estimated
	 * by indexAM since we should consider index traversal related costs too.
	 */
	Cost columnarIndexScanCost = ColumnarIndexScanAdditionalCost(root, rel, relationId,
																 indexPath);
	indexPath->path.total_cost += columnarIndexScanCost;

	ereport(DEBUG4, (errmsg("columnar table index scan costs re-estimated "
							"by columnarAM (including indexAM costs): "
							"startup cost = %.10f, total cost = %.10f",
							indexPath->path.startup_cost,
							indexPath->path.total_cost)));
}


/*
 * ColumnarIndexScanAdditionalCost returns additional cost estimated for
 * index scan described by IndexPath for columnar table with relationId.
 */
static Cost
ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
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
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

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
	double absIndexCorrelation = float_abs(indexCorrelation);

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

	/* even in the best case, we will read a single stripe */
	estimatedStripeReadCount = Max(estimatedStripeReadCount, 1.0);

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
 * CostColumnarSeqPath sets costs given seq path for columnar table with
 * relationId.
 */
static void
CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path)
{
	if (!enable_seqscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	/*
	 * Seq scan doesn't support projection or qual pushdown, so we will read
	 * all the stripes and all the columns.
	 */
	double stripesToRead = ColumnarTableStripeCount(relationId);
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);

	path->startup_cost = 0;
	path->total_cost = stripesToRead *
					   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * RelationIdGetNumberOfAttributes returns number of attributes that relation
 * with relationId has.
 */
static int
RelationIdGetNumberOfAttributes(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	int nattrs = relation->rd_att->natts;
	RelationClose(relation);
	return nattrs;
}


/*
 * CheckVarStats() checks whether a qual involving this Var is likely to be
 * useful based on the correlation stats. If so, or if stats are unavailable,
 * return true; otherwise return false and sets absVarCorrelation in case
 * caller wants to use for logging purposes.
 */
static bool
CheckVarStats(PlannerInfo *root, Var *var, Oid sortop, float4 *absVarCorrelation)
{
	/*
	 * Collect isunique, ndistinct, and varCorrelation.
	 */
	VariableStatData varStatData;
	examine_variable(root, (Node *) var, var->varno, &varStatData);
	if (varStatData.rel == NULL ||
		!HeapTupleIsValid(varStatData.statsTuple))
	{
		return true;
	}

	AttStatsSlot sslot;
	if (!get_attstatsslot(&sslot, varStatData.statsTuple,
						  STATISTIC_KIND_CORRELATION, sortop,
						  ATTSTATSSLOT_NUMBERS))
	{
		ReleaseVariableStats(varStatData);
		return true;
	}

	Assert(sslot.nnumbers == 1);

	float4 varCorrelation = sslot.numbers[0];

	ReleaseVariableStats(varStatData);

	/*
	 * If the Var is not highly correlated, then the chunk's min/max bounds
	 * will be nearly useless.
	 */
	if (float_abs(varCorrelation) < ColumnarQualPushdownCorrelationThreshold)
	{
		if (absVarCorrelation)
		{
			/*
			 * Report absVarCorrelation if caller wants to know why given
			 * var is rejected.
			 */
			*absVarCorrelation = float_abs(varCorrelation);
		}
		return false;
	}

	return true;
}


/*
 * ExprReferencesRelid returns true if any of the Expr's Vars refer to the
 * given relid; false otherwise.
 */
static bool
ExprReferencesRelid(Expr *expr, Index relid)
{
	List *exprVars = pull_var_clause(
		(Node *) expr, PVC_RECURSE_AGGREGATES |
		PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS);
	ListCell *lc;
	foreach(lc, exprVars)
	{
		Var *var = (Var *) lfirst(lc);
		if (var->varno == relid)
		{
			return true;
		}
	}

	return false;
}


/*
 * ExtractPushdownClause extracts an Expr node from given clause for pushing down
 * into the given rel (including join clauses). This test may not be exact in
 * all cases; it's used to reduce the search space for parameterization.
 *
 * Note that we don't try to handle cases like "Var + ExtParam = 3". That
 * would require going through eval_const_expression after parameter binding,
 * and that doesn't seem worth the effort. Here we just look for "Var op Expr"
 * or "Expr op Var", where Var references rel and Expr references other rels
 * (or no rels at all).
 *
 * Moreover, this function also looks into BoolExpr's to recursively extract
 * pushdownable OpExpr's of them:
 * i)   AND_EXPR:
 *      Take pushdownable args of AND expressions by ignoring the other args.
 * ii)  OR_EXPR:
 *      Ignore the whole OR expression if we cannot exract a pushdownable Expr
 *      from one of its args.
 * iii) NOT_EXPR:
 *      Simply ignore NOT expressions since we don't expect to see them before
 *      an expression that we can pushdown, see the comment in function.
 *
 * The reasoning for those three rules could also be summarized as such;
 * for any expression that we cannot push-down, we must assume that it
 * evaluates to true.
 *
 * For example, given following WHERE clause:
 * (
 *     (a > random() OR a < 30)
 *     AND
 *     a < 200
 * ) OR
 * (
 *     a = 300
 *     OR
 *     a > 400
 * );
 * Even if we can pushdown (a < 30), we cannot pushdown (a > random() OR a < 30)
 * due to (a > random()). However, we can pushdown (a < 200), so we extract
 * (a < 200) from the lhs of the top level OR expression.
 *
 * For the rhs of the top level OR expression, since we can pushdown both (a = 300)
 * and (a > 400), we take this part as is.
 *
 * Finally, since both sides of the top level OR expression yielded pushdownable
 * expressions, we will pushdown the following:
 *  (a < 200) OR ((a = 300) OR (a > 400))
 */
static Expr *
ExtractPushdownClause(PlannerInfo *root, RelOptInfo *rel, Node *node)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr *boolExpr = castNode(BoolExpr, node);
		if (boolExpr->boolop == NOT_EXPR)
		{
			/*
			 * Standard planner should have already applied de-morgan rule to
			 * simple NOT expressions. If we encounter with such an expression
			 * here, then it can't be a pushdownable one, such as:
			 *   WHERE id NOT IN (SELECT id FROM something).
			 */
			ereport(ColumnarPlannerDebugLevel,
					(errmsg("columnar planner: cannot push down clause: "
							"must not contain a subplan")));
			return NULL;
		}

		List *pushdownableArgs = NIL;

		Node *boolExprArg = NULL;
		foreach_declared_ptr(boolExprArg, boolExpr->args)
		{
			Expr *pushdownableArg = ExtractPushdownClause(root, rel,
														  (Node *) boolExprArg);
			if (pushdownableArg)
			{
				pushdownableArgs = lappend(pushdownableArgs, pushdownableArg);
			}
			else if (boolExpr->boolop == OR_EXPR)
			{
				ereport(ColumnarPlannerDebugLevel,
						(errmsg("columnar planner: cannot push down clause: "
								"all arguments of an OR expression must be "
								"pushdownable but one of them was not, due "
								"to the reason given above")));
				return NULL;
			}

			/* simply skip AND args that we cannot pushdown */
		}

		int npushdownableArgs = list_length(pushdownableArgs);
		if (npushdownableArgs == 0)
		{
			ereport(ColumnarPlannerDebugLevel,
					(errmsg("columnar planner: cannot push down clause: "
							"none of the arguments were pushdownable, "
							"due to the reason(s) given above ")));
			return NULL;
		}
		else if (npushdownableArgs == 1)
		{
			return (Expr *) linitial(pushdownableArgs);
		}

		if (boolExpr->boolop == AND_EXPR)
		{
			return make_andclause(pushdownableArgs);
		}
		else if (boolExpr->boolop == OR_EXPR)
		{
			return make_orclause(pushdownableArgs);
		}
		else
		{
			/* already discarded NOT expr, so should not be reachable */
			return NULL;
		}
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		if (!contain_volatile_functions(node))
		{
			return (Expr *) node;
		}
		else
		{
			return NULL;
		}
	}

	if (!IsA(node, OpExpr) || list_length(((OpExpr *) node)->args) != 2)
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"must be binary operator expression")));
		return NULL;
	}

	OpExpr *opExpr = castNode(OpExpr, node);
	Expr *lhs = list_nth(opExpr->args, 0);
	Expr *rhs = list_nth(opExpr->args, 1);

	Var *varSide;
	Expr *exprSide;

	if (IsA(lhs, Var) && ((Var *) lhs)->varno == rel->relid &&
		!ExprReferencesRelid((Expr *) rhs, rel->relid))
	{
		varSide = castNode(Var, lhs);
		exprSide = rhs;
	}
	else if (IsA(rhs, Var) && ((Var *) rhs)->varno == rel->relid &&
			 !ExprReferencesRelid((Expr *) lhs, rel->relid))
	{
		varSide = castNode(Var, rhs);
		exprSide = lhs;
	}
	else
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"must match 'Var <op> Expr' or 'Expr <op> Var'"),
				 errhint("Var must only reference this rel, "
						 "and Expr must not reference this rel")));
		return NULL;
	}

	if (varSide->varattno <= 0)
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"var is whole-row reference or system column")));
		return NULL;
	}

	if (contain_volatile_functions((Node *) exprSide))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"expr contains volatile functions")));
		return NULL;
	}

	/* only the default opclass is used for qual pushdown. */
	Oid varOpClass = GetDefaultOpClass(varSide->vartype, BTREE_AM_OID);
	Oid varOpFamily;
	Oid varOpcInType;

	if (!OidIsValid(varOpClass) ||
		!get_opclass_opfamily_and_input_type(varOpClass, &varOpFamily,
											 &varOpcInType))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"cannot find default btree opclass and opfamily for type: %s",
						format_type_be(varSide->vartype))));
		return NULL;
	}

	if (!op_in_opfamily(opExpr->opno, varOpFamily))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"operator %d not a member of opfamily %d",
						opExpr->opno, varOpFamily)));
		return NULL;
	}

	Oid sortop = get_opfamily_member(varOpFamily, varOpcInType,
									 varOpcInType, BTLessStrategyNumber);
	Assert(OidIsValid(sortop));

	/*
	 * Check that statistics on the Var support the utility of this
	 * clause.
	 */
	float4 absVarCorrelation = 0;
	if (!CheckVarStats(root, varSide, sortop, &absVarCorrelation))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"absolute correlation (%.3f) of var attribute %d is "
						"smaller than the value configured in "
						"\"columnar.qual_pushdown_correlation_threshold\" "
						"(%.3f)", absVarCorrelation, varSide->varattno,
						ColumnarQualPushdownCorrelationThreshold)));
		return NULL;
	}

	return (Expr *) node;
}


/*
 * FilterPushdownClauses filters for clauses that are candidates for pushing
 * down into rel.
 */
static List *
FilterPushdownClauses(PlannerInfo *root, RelOptInfo *rel, List *inputClauses)
{
	List *filteredClauses = NIL;
	ListCell *lc;
	foreach(lc, inputClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Ignore clauses that don't refer to this rel, and pseudoconstants.
		 *
		 * XXX: A pseudoconstant may be of use, but it doesn't make sense to
		 * push it down because it doesn't contain any Vars. Look into if
		 * there's something we should do with pseudoconstants here.
		 */
		if (rinfo->pseudoconstant ||
			!bms_is_member(rel->relid, rinfo->required_relids))
		{
			continue;
		}

		Expr *pushdownableExpr = ExtractPushdownClause(root, rel, (Node *) rinfo->clause);
		if (!pushdownableExpr)
		{
			continue;
		}

		rinfo = copyObject(rinfo);
		rinfo->clause = pushdownableExpr;
		filteredClauses = lappend(filteredClauses, rinfo);
	}

	return filteredClauses;
}


/*
 * PushdownJoinClauseMatches is a callback that returns true, indicating that
 * we want all of the clauses from generate_implied_equalities_for_column().
 */
static bool
PushdownJoinClauseMatches(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	return true;
}


/*
 * FindPushdownJoinClauses finds join clauses, including those implied by ECs,
 * that may be pushed down.
 */
static List *
FindPushdownJoinClauses(PlannerInfo *root, RelOptInfo *rel)
{
	List *joinClauses = copyObject(rel->joininfo);

	/*
	 * Here we are generating the clauses just so we can later extract the
	 * interesting relids. This is somewhat wasteful, but it allows us to
	 * filter out joinclauses, reducing the number of relids we need to
	 * consider.
	 *
	 * XXX: also find additional clauses for joininfo that are implied by ECs?
	 */
	List *ecClauses = generate_implied_equalities_for_column(
		root, rel, PushdownJoinClauseMatches, NULL,
		rel->lateral_referencers);
	List *allClauses = list_concat(joinClauses, ecClauses);

	return FilterPushdownClauses(root, rel, allClauses);
}


/*
 * FindCandidateRelids identifies candidate rels for parameterization from the
 * list of join clauses.
 *
 * Some rels cannot be considered for parameterization, such as a partitioned
 * parent of the given rel. Other rels are just not useful because they don't
 * appear in a join clause that could be pushed down.
 */
static Relids
FindCandidateRelids(PlannerInfo *root, RelOptInfo *rel, List *joinClauses)
{
	Relids candidateRelids = NULL;
	ListCell *lc;
	foreach(lc, joinClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		candidateRelids = bms_add_members(candidateRelids,
										  rinfo->required_relids);
	}

	candidateRelids = bms_del_members(candidateRelids, rel->relids);
	candidateRelids = bms_del_members(candidateRelids, rel->lateral_relids);
	return candidateRelids;
}


/*
 * Combinations() calculates the number of combinations of n things taken k at
 * a time. When the correct result is large, the calculation may produce a
 * non-integer result, or overflow to inf, which caller should handle
 * appropriately.
 *
 * Use the following two formulae from Knuth TAoCP, 1.2.6:
 *    (2) Combinations(n, k) = (n*(n-1)..(n-k+1)) / (k*(k-1)..1)
 *    (5) Combinations(n, k) = Combinations(n, n-k)
 */
static double
Combinations(int n, int k)
{
	double v = 1;

	/*
	 * If k is close to n, then both the numerator and the denominator are
	 * close to n!, and we may overflow even if the input is reasonable
	 * (e.g. Combinations(500, 500)). Use formula (5) to choose the smaller,
	 * but equivalent, k.
	 */
	k = Min(k, n - k);

	/* calculate numerator of formula (2) first */
	for (int i = n; i >= n - k + 1; i--)
	{
		v *= i;
	}

	/*
	 * Divide by each factor in the denominator of formula (2), skipping
	 * division by 1.
	 */
	for (int i = k; i >= 2; i--)
	{
		v /= i;
	}

	return v;
}


/*
 * ChooseDepthLimit() calculates the depth limit for the parameterization
 * search, given the number of candidate relations.
 *
 * The maximum number of paths generated for a given depthLimit is:
 *
 *    Combinations(nCandidates, 0) + Combinations(nCandidates, 1) + ... +
 *    Combinations(nCandidates, depthLimit)
 *
 * There's no closed formula for a partial sum of combinations, so just keep
 * increasing the depth until the number of combinations exceeds the limit.
 */
static int
ChooseDepthLimit(int nCandidates)
{
	if (!EnableColumnarQualPushdown)
	{
		return 0;
	}

	int depth = 0;
	double numPaths = 1;

	while (depth < nCandidates)
	{
		numPaths += Combinations(nCandidates, depth + 1);

		if (numPaths > (double) ColumnarMaxCustomScanPaths)
		{
			break;
		}

		depth++;
	}

	return depth;
}


/*
 * AddColumnarScanPaths is the entry point for recursively generating
 * parameterized paths. See AddColumnarScanPathsRec() for discussion.
 */
static void
AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	List *joinClauses = FindPushdownJoinClauses(root, rel);
	Relids candidateRelids = FindCandidateRelids(root, rel, joinClauses);

	int depthLimit = ChooseDepthLimit(bms_num_members(candidateRelids));

	/* must always parameterize by lateral refs */
	Relids paramRelids = bms_copy(rel->lateral_relids);

	AddColumnarScanPathsRec(root, rel, rte, paramRelids, candidateRelids,
							depthLimit);
}


/*
 * AddColumnarScanPathsRec is a recursive function to search the
 * parameterization space and add CustomPaths for columnar scans.
 *
 * The set paramRelids is the parameterization at the current level, and
 * candidateRelids is the set from which we draw to generate paths with
 * greater parameterization.
 *
 * Columnar tables resemble indexes because of the ability to push down
 * quals. Ordinary quals, such as x = 7, can be pushed down easily. But join
 * quals of the form "x = y" (where "y" comes from another rel) require the
 * proper parameterization.
 *
 * Paths that require more outer rels can push down more join clauses that
 * depend on those outer rels. But requiring more outer rels gives the planner
 * fewer options for the shape of the plan. That means there is a trade-off,
 * and we should generate plans of various parameterizations, then let the
 * planner choose. We always need to generate one minimally-parameterized path
 * (parameterized only by lateral refs, if present) to make sure that at least
 * one path can be chosen. Then, we generate as many parameterized paths as we
 * reasonably can.
 *
 * The set of all possible parameterizations is the power set of
 * candidateRelids. The power set has cardinality 2^N, where N is the
 * cardinality of candidateRelids. To avoid creating a huge number of paths,
 * limit the depth of the search; the depthLimit is equivalent to the maximum
 * number of required outer rels (beyond the minimal parameterization) for the
 * path. A depthLimit of zero means that only the minimally-parameterized path
 * will be generated.
 */
static void
AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
						Relids paramRelids, Relids candidateRelids,
						int depthLimit)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	Assert(!bms_overlap(paramRelids, candidateRelids));
	AddColumnarScanPath(root, rel, rte, paramRelids);

	/* recurse for all candidateRelids, unless we hit the depth limit */
	Assert(depthLimit >= 0);
	if (depthLimit-- == 0)
	{
		return;
	}

	/*
	 * Iterate through parameter combinations depth-first. Deeper levels
	 * generate paths of greater parameterization (and hopefully lower
	 * cost).
	 */
	Relids tmpCandidateRelids = bms_copy(candidateRelids);
	int relid = -1;
	while ((relid = bms_next_member(candidateRelids, relid)) >= 0)
	{
		Relids tmpParamRelids = bms_add_member(
			bms_copy(paramRelids), relid);

		/*
		 * Because we are generating combinations (not permutations), remove
		 * the relid from the set of candidates at this level as we descend to
		 * the next.
		 */
		tmpCandidateRelids = bms_del_member(tmpCandidateRelids, relid);

		AddColumnarScanPathsRec(root, rel, rte, tmpParamRelids,
								tmpCandidateRelids, depthLimit);
	}

	bms_free(tmpCandidateRelids);
}


/*
 * ParameterizationAsString returns the string representation of the set of
 * rels given in paramRelids.
 *
 * Takes a StringInfo so that it doesn't return palloc'd memory. This makes it
 * easy to call this function as an argument to ereport(), such that it won't
 * be evaluated unless the message is going to be output somewhere.
 */
static char *
ParameterizationAsString(PlannerInfo *root, Relids paramRelids, StringInfo buf)
{
	bool firstTime = true;
	int relid = -1;

	if (bms_num_members(paramRelids) == 0)
	{
		return "unparameterized";
	}

	appendStringInfoString(buf, "parameterized by rels {");
	while ((relid = bms_next_member(paramRelids, relid)) >= 0)
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		const char *relname = quote_identifier(rte->eref->aliasname);

		appendStringInfo(buf, "%s%s", firstTime ? "" : ", ", relname);

		if (relname != rte->eref->aliasname)
		{
			pfree((void *) relname);
		}

		firstTime = false;
	}
	appendStringInfoString(buf, "}");
	return buf->data;
}


/*
 * ContainsExecParams tests whether the node contains any exec params. The
 * signature accepts an extra argument for use with expression_tree_walker.
 */
static bool
ContainsExecParams(Node *node, void *notUsed)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		if (param->paramkind == PARAM_EXEC)
		{
			return true;
		}
	}
	return expression_tree_walker(node, ContainsExecParams, NULL);
}


/*
 * Create and add a path with the given parameterization paramRelids.
 *
 * XXX: Consider refactoring to be more like postgresGetForeignPaths(). The
 * only differences are param_info and custom_private.
 */
static void
AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
					Relids paramRelids)
{
	/*
	 * Must return a CustomPath, not a larger structure containing a
	 * CustomPath as the first field. Otherwise, nodeToString() will fail to
	 * output the additional fields.
	 */
	CustomPath *cpath = makeNode(CustomPath);

	cpath->methods = &ColumnarScanPathMethods;

	/* necessary to avoid extra Result node in PG15 */
	cpath->flags = CUSTOMPATH_SUPPORT_PROJECTION;

	/*
	 * populate generic path information
	 */
	Path *path = &cpath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;

	/* columnar scans are not parallel-aware, but they are parallel-safe */
	path->parallel_safe = rel->consider_parallel;

	path->param_info = get_baserel_parampathinfo(root, rel, paramRelids);

	/*
	 * Usable clauses for this parameterization exist in baserestrictinfo and
	 * ppi_clauses.
	 */
	List *allClauses = copyObject(rel->baserestrictinfo);
	if (path->param_info != NULL)
	{
		allClauses = list_concat(allClauses, path->param_info->ppi_clauses);
	}

	allClauses = FilterPushdownClauses(root, rel, allClauses);

	/*
	 * Plain clauses may contain extern params, but not exec params, and can
	 * be evaluated at init time or rescan time. Track them in another list
	 * that is a subset of allClauses.
	 *
	 * Note: although typically baserestrictinfo contains plain clauses,
	 * that's not always true. It can also contain a qual referencing a Var at
	 * a higher query level, which can be turned into an exec param, and
	 * therefore it won't be a plain clause.
	 */
	List *plainClauses = NIL;
	ListCell *lc;
	foreach(lc, allClauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		if (bms_is_subset(rinfo->required_relids, rel->relids) &&
			!ContainsExecParams((Node *) rinfo->clause, NULL))
		{
			plainClauses = lappend(plainClauses, rinfo);
		}
	}

	/*
	 * We can't make our own CustomPath structure, so we need to put
	 * everything in the custom_private list. To keep the two lists separate,
	 * we make them sublists in a 2-element list.
	 */
	if (EnableColumnarQualPushdown)
	{
		cpath->custom_private = list_make2(copyObject(plainClauses),
										   copyObject(allClauses));
	}
	else
	{
		cpath->custom_private = list_make2(NIL, NIL);
	}

	int numberOfColumnsRead = 0;
#if PG_VERSION_NUM >= PG_VERSION_16
	if (rte->perminfoindex > 0)
	{
		/*
		 * If perminfoindex > 0, that means that this relation's permission info
		 * is directly found in the list of rteperminfos of the Query(root->parse)
		 * So, all we have to do here is retrieve that info.
		 */
		RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos,
														   rte);
		numberOfColumnsRead = bms_num_members(perminfo->selectedCols);
	}
	else
	{
		/*
		 * If perminfoindex = 0, that means we are skipping the check for permission info
		 * for this relation, which means that it's either a partition or an inheritance child.
		 * In these cases, we need to access the permission info of the top parent of this relation.
		 * After thorough checking, we found that the index of the top parent pointing to the correct
		 * range table entry in Query's range tables (root->parse->rtable) is found under
		 * RelOptInfo rel->top_parent->relid.
		 * For reference, check expand_partitioned_rtentry and expand_inherited_rtentry PG functions
		 */
		Assert(rel->top_parent);
		RangeTblEntry *parent_rte = rt_fetch(rel->top_parent->relid, root->parse->rtable);
		RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos,
														   parent_rte);
		numberOfColumnsRead = bms_num_members(fixup_inherited_columns(perminfo->relid,
																	  rte->relid,
																	  perminfo->
																	  selectedCols));
	}
#else
	numberOfColumnsRead = bms_num_members(rte->selectedCols);
#endif

	int numberOfClausesPushed = list_length(allClauses);

	CostColumnarScan(root, rel, rte->relid, cpath, numberOfColumnsRead,
					 numberOfClausesPushed);


	StringInfoData buf;
	initStringInfo(&buf);
	ereport(ColumnarPlannerDebugLevel,
			(errmsg("columnar planner: adding CustomScan path for %s",
					rte->eref->aliasname),
			 errdetail("%s; %d clauses pushed down",
					   ParameterizationAsString(root, paramRelids, &buf),
					   numberOfClausesPushed)));

	add_path(rel, path);
}


#if PG_VERSION_NUM >= PG_VERSION_16

/*
 * fixup_inherited_columns
 *
 * Exact function Copied from PG16 as it's static.
 *
 * When user is querying on a table with children, it implicitly accesses
 * child tables also. So, we also need to check security label of child
 * tables and columns, but there is no guarantee attribute numbers are
 * same between the parent and children.
 * It returns a bitmapset which contains attribute number of the child
 * table based on the given bitmapset of the parent.
 */
static Bitmapset *
fixup_inherited_columns(Oid parentId, Oid childId, Bitmapset *columns)
{
	Bitmapset *result = NULL;

	/*
	 * obviously, no need to do anything here
	 */
	if (parentId == childId)
	{
		return columns;
	}

	int index = -1;
	while ((index = bms_next_member(columns, index)) >= 0)
	{
		/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
		AttrNumber attno = index + FirstLowInvalidHeapAttributeNumber;

		/*
		 * whole-row-reference shall be fixed-up later
		 */
		if (attno == InvalidAttrNumber)
		{
			result = bms_add_member(result, index);
			continue;
		}

		char *attname = get_attname(parentId, attno, false);
		attno = get_attnum(childId, attname);
		if (attno == InvalidAttrNumber)
		{
			elog(ERROR, "cache lookup failed for attribute %s of relation %u",
				 attname, childId);
		}

		result = bms_add_member(result,
								attno - FirstLowInvalidHeapAttributeNumber);

		pfree(attname);
	}

	return result;
}


#endif


/*
 * CostColumnarScan calculates the cost of scanning the columnar table. The
 * cost is estimated by using all stripe metadata to estimate based on the
 * columns to read how many pages need to be read.
 */
static void
CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
				 CustomPath *cpath, int numberOfColumnsRead, int nClauses)
{
	Path *path = &cpath->path;

	List *allClauses = lsecond(cpath->custom_private);
	Selectivity clauseSel = clauselist_selectivity(
		root, allClauses, rel->relid, JOIN_INNER, NULL);

	/*
	 * We already filtered out clauses where the overall selectivity would be
	 * misleading, such as inequalities involving an uncorrelated column. So
	 * we can apply the selectivity directly to the number of stripes.
	 */
	double stripesToRead = clauseSel * ColumnarTableStripeCount(relationId);
	stripesToRead = Max(stripesToRead, 1.0);

	path->rows = rel->rows;
	path->startup_cost = 0;
	path->total_cost = stripesToRead *
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
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *stripeList = StripesForRelfilelocator(RelationPhysicalIdentifier_compat(
													relation));
	RelationClose(relation);

	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	StripeMetadata *stripeMetadata = NULL;
	foreach_declared_ptr(stripeMetadata, stripeList)
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
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *stripeList = StripesForRelfilelocator(RelationPhysicalIdentifier_compat(
													relation));
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
	CustomScan *cscan = makeNode(CustomScan);

	cscan->methods = &ColumnarScanScanMethods;

	/* XXX: also need to store projected column list for EXPLAIN */

	if (EnableColumnarQualPushdown)
	{
		/*
		 * Lists of pushed-down clauses. The Vars in custom_exprs referencing
		 * other relations will be changed into exec Params by
		 * create_customscan_plan().
		 *
		 * Like CustomPath->custom_private, keep a list of plain clauses
		 * separate from the list of all clauses by making them sublists of a
		 * 2-element list.
		 *
		 * XXX: custom_exprs are the quals that will be pushed into the
		 * columnar reader code; some of these may not be usable. We should
		 * fix this by processing the quals more completely and using
		 * ScanKeys.
		 */
		List *plainClauses = extract_actual_clauses(
			linitial(best_path->custom_private), false /* no pseudoconstants */);
		List *allClauses = extract_actual_clauses(
			lsecond(best_path->custom_private), false /* no pseudoconstants */);
		cscan->custom_exprs = copyObject(list_make2(plainClauses, allClauses));
	}
	else
	{
		cscan->custom_exprs = list_make2(NIL, NIL);
	}

	cscan->scan.plan.qual = extract_actual_clauses(
		clauses, false /* no pseudoconstants */);
	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.scanrelid = best_path->path.parent->relid;

#if (PG_VERSION_NUM >= 150000)

	/* necessary to avoid extra Result node in PG15 */
	cscan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
#endif

	return (Plan *) cscan;
}


/*
 * ReparameterizeMutator changes all varnos referencing the topmost parent of
 * child_rel to instead reference child_rel directly.
 */
static Node *
ReparameterizeMutator(Node *node, RelOptInfo *child_rel)
{
	if (node == NULL)
	{
		return NULL;
	}
	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		if (bms_is_member(var->varno, child_rel->top_parent_relids))
		{
			var = copyObject(var);
			var->varno = child_rel->relid;
		}
		return (Node *) var;
	}

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = castNode(RestrictInfo, node);
		rinfo = copyObject(rinfo);
		rinfo->clause = (Expr *) expression_tree_mutator(
			(Node *) rinfo->clause, ReparameterizeMutator, (void *) child_rel);
		return (Node *) rinfo;
	}
	return expression_tree_mutator(node, ReparameterizeMutator,
								   (void *) child_rel);
}


/*
 * ColumnarScanPath_ReparameterizeCustomPathByChild is a method called when a
 * path is reparameterized directly to a child relation, rather than the
 * top-level parent.
 *
 * For instance, let there be a join of two partitioned columnar relations PX
 * and PY. A path for a ColumnarScan of PY3 might be parameterized by PX so
 * that the join qual "PY3.a = PX.a" (referencing the parent PX) can be pushed
 * down. But if the planner decides on a partition-wise join, then the path
 * will be reparameterized on the child table PX3 directly.
 *
 * When that happens, we need to update all Vars in the pushed-down quals to
 * reference PX3, not PX, to match the new parameterization. This method
 * notifies us that it needs to be done, and allows us to update the
 * information in custom_private.
 */
static List *
ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
												 List *custom_private,
												 RelOptInfo *child_rel)
{
	return (List *) ReparameterizeMutator((Node *) custom_private, child_rel);
}


static Node *
ColumnarScan_CreateCustomScanState(CustomScan *cscan)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) newNode(
		sizeof(ColumnarScanState), T_CustomScanState);

	CustomScanState *cscanstate = &columnarScanState->custom_scanstate;
	cscanstate->methods = &ColumnarScanExecuteMethods;

	return (Node *) cscanstate;
}


/*
 * EvalParamsMutator evaluates Params in the expression and replaces them with
 * Consts.
 */
static Node *
EvalParamsMutator(Node *node, ExprContext *econtext)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int16 typLen;
		bool typByVal;
		bool isnull;

		get_typlenbyval(param->paramtype, &typLen, &typByVal);

		/* XXX: should save ExprState for efficiency */
		ExprState *exprState = ExecInitExprWithParams((Expr *) node,
													  econtext->ecxt_param_list_info);
		Datum pval = ExecEvalExpr(exprState, econtext, &isnull);

		return (Node *) makeConst(param->paramtype,
								  param->paramtypmod,
								  param->paramcollid,
								  (int) typLen,
								  pval,
								  isnull,
								  typByVal);
	}

	return expression_tree_mutator(node, EvalParamsMutator, (void *) econtext);
}


static void
ColumnarScan_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags)
{
	CustomScan *cscan = (CustomScan *) cscanstate->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) cscanstate;
	ExprContext *stdecontext = cscanstate->ss.ps.ps_ExprContext;

	/*
	 * Make a new ExprContext just like the existing one, except that we don't
	 * reset it every tuple.
	 */
	ExecAssignExprContext(estate, &cscanstate->ss.ps);
	columnarScanState->css_RuntimeContext = cscanstate->ss.ps.ps_ExprContext;
	cscanstate->ss.ps.ps_ExprContext = stdecontext;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *plainClauses = linitial(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) plainClauses, columnarScanState->css_RuntimeContext);

	/* scan slot is already initialized */
}


/*
 * ColumnarAttrNeeded returns a list of AttrNumber's for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
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
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *allClauses = lsecond(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) allClauses, columnarScanState->css_RuntimeContext);

	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	if (scanDesc != NULL)
	{
		/* XXX: hack to pass quals as scan keys */
		ScanKey scanKeys = (ScanKey) columnarScanState->qual;
		table_rescan(node->ss.ss_currentScanDesc,
					 scanKeys);
	}
}


static void
ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
							   ExplainState *es)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	List *context = set_deparse_context_planstate(
		es->deparse_cxt, (Node *) &node->ss.ps, ancestors);

	List *projectedColumns = ColumnarVarNeeded(columnarScanState);
	const char *projectedColumnsStr = ColumnarProjectedColumnsStr(
		context, projectedColumns);
	ExplainPropertyText("Columnar Projected Columns",
						projectedColumnsStr, es);

	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List *chunkGroupFilter = lsecond(cscan->custom_exprs);
	if (chunkGroupFilter != NULL)
	{
		const char *pushdownClausesStr = ColumnarPushdownClausesStr(
			context, chunkGroupFilter);
		ExplainPropertyText("Columnar Chunk Group Filters",
							pushdownClausesStr, es);

		ColumnarScanDesc columnarScanDesc =
			(ColumnarScanDesc) node->ss.ss_currentScanDesc;
		if (columnarScanDesc != NULL)
		{
			ExplainPropertyInteger(
				"Columnar Chunk Groups Removed by Filter",
				NULL, ColumnarScanChunkGroupsFiltered(columnarScanDesc), es);
		}
	}
}


/*
 * ColumnarPushdownClausesStr represents the clauses to push down as a string.
 */
static const char *
ColumnarPushdownClausesStr(List *context, List *clauses)
{
	Expr *conjunction;

	Assert(list_length(clauses) > 0);

	if (list_length(clauses) == 1)
	{
		conjunction = (Expr *) linitial(clauses);
	}
	else
	{
		conjunction = make_andclause(clauses);
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) conjunction, context,
							  useTableNamePrefix, showImplicitCast);
}


/*
 * ColumnarProjectedColumnsStr generates projected column string for
 * explain output.
 */
static const char *
ColumnarProjectedColumnsStr(List *context, List *projectedColumns)
{
	if (list_length(projectedColumns) == 0)
	{
		return "<columnar optimized out all columns>";
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) projectedColumns, context,
							  useTableNamePrefix, showImplicitCast);
}


/*
 * ColumnarVarNeeded returns a list of Var objects for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
static List *
ColumnarVarNeeded(ColumnarScanState *columnarScanState)
{
	ScanState *scanState = &columnarScanState->custom_scanstate.ss;

	List *varList = NIL;

	Bitmapset *neededAttrSet = ColumnarAttrNeeded(scanState);
	int bmsMember = -1;
	while ((bmsMember = bms_next_member(neededAttrSet, bmsMember)) >= 0)
	{
		Relation columnarRelation = scanState->ss_currentRelation;

		/* neededAttrSet already represents 0-indexed attribute numbers */
		Form_pg_attribute columnForm =
			TupleDescAttr(RelationGetDescr(columnarRelation), bmsMember);
		if (columnForm->attisdropped)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is dropped",
								   bmsMember + 1,
								   RelationGetRelationName(columnarRelation))));
		}
		else if (columnForm->attnum <= 0)
		{
			/*
			 * ColumnarAttrNeeded should have already thrown an error for
			 * system columns. Similarly, it should have already expanded
			 * whole-row references to individual attributes.
			 */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is either "
								   "a system column or a whole-row "
								   "reference", columnForm->attnum,
								   RelationGetRelationName(columnarRelation))));
		}


		/*
		 * varlevelsup is used to figure out the (query) level of the Var
		 * that we are investigating. Since we are dealing with a particular
		 * relation, it is useless here.
		 */
		Index varlevelsup = 0;

		CustomScanState *customScanState = (CustomScanState *) columnarScanState;
		CustomScan *customScan = (CustomScan *) customScanState->ss.ps.plan;
		Index scanrelid = customScan->scan.scanrelid;
		Var *var = makeVar(scanrelid, columnForm->attnum, columnForm->atttypid,
						   columnForm->atttypmod, columnForm->attcollation,
						   varlevelsup);
		varList = lappend(varList, var);
	}

	return varList;
}


/*
 * set_deparse_context_planstate is a compatibility wrapper for versions 13+.
 */
static List *
set_deparse_context_planstate(List *dpcontext, Node *node, List *ancestors)
{
	PlanState *ps = (PlanState *) node;
	return set_deparse_context_plan(dpcontext, ps->plan, ancestors);
}
