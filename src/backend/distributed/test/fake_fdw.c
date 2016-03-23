/*-------------------------------------------------------------------------
 *
 * test/src/fake_fdw.c
 *
 * This file contains a barebones FDW implementation, suitable for use in
 * test code. Inspired by Andrew Dunstan's blackhole_fdw.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include <stddef.h>

#include "distributed/test_helper_functions.h" /* IWYU pragma: keep */
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/palloc.h"

/* local function forward declarations */
static void FakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								  Oid foreigntableid);
static void FakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								Oid foreigntableid);
#if (PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 90500)
static ForeignScan * FakeGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										Oid foreigntableid, ForeignPath *best_path,
										List *tlist, List *scan_clauses);
#else
static ForeignScan * FakeGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										Oid foreigntableid, ForeignPath *best_path,
										List *tlist, List *scan_clauses,
										Plan *outer_plan);
#endif
static void FakeBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot * FakeIterateForeignScan(ForeignScanState *node);
static void FakeReScanForeignScan(ForeignScanState *node);
static void FakeEndForeignScan(ForeignScanState *node);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(fake_fdw_handler);


/*
 * fake_fdw_handler populates an FdwRoutine with pointers to the functions
 * implemented within this file.
 */
Datum
fake_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = FakeGetForeignRelSize;
	fdwroutine->GetForeignPaths = FakeGetForeignPaths;
	fdwroutine->GetForeignPlan = FakeGetForeignPlan;
	fdwroutine->BeginForeignScan = FakeBeginForeignScan;
	fdwroutine->IterateForeignScan = FakeIterateForeignScan;
	fdwroutine->ReScanForeignScan = FakeReScanForeignScan;
	fdwroutine->EndForeignScan = FakeEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}


/*
 * FakeGetForeignRelSize populates baserel with a fake relation size.
 */
static void
FakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	baserel->rows = 0;
	baserel->fdw_private = (void *) palloc0(1);
}


/*
 * FakeGetForeignPaths adds a single fake foreign path to baserel.
 */
static void
FakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost startup_cost = 0;
	Cost total_cost = startup_cost + baserel->rows;

#if (PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 90500)
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel, baserel->rows,
													   startup_cost, total_cost, NIL,
													   NULL, NIL));
#else
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel, baserel->rows,
													   startup_cost, total_cost, NIL,
													   NULL, NULL, NIL));
#endif
}


/*
 * FakeGetForeignPlan builds a fake foreign plan.
 */
#if (PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 90500)
static ForeignScan *
FakeGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
				   ForeignPath *best_path, List *tlist, List *scan_clauses)
#else
static ForeignScan *
FakeGetForeignPlan(PlannerInfo * root, RelOptInfo * baserel, Oid foreigntableid,
				   ForeignPath * best_path, List * tlist, List * scan_clauses,
				   Plan * outer_plan)
#endif
{
	Index scan_relid = baserel->relid;
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* make_foreignscan has a different signature in 9.3 and 9.4 than in 9.5 */
#if (PG_VERSION_NUM >= 90300 && PG_VERSION_NUM < 90500)
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL);
#else
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL, NIL, NIL,
							outer_plan);
#endif
}


/*
 * FakeBeginForeignScan begins the fake plan (i.e. does nothing).
 */
static void
FakeBeginForeignScan(ForeignScanState *node, int eflags) { }


/*
 * FakeIterateForeignScan continues the fake plan (i.e. does nothing).
 */
static TupleTableSlot *
FakeIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);

	return slot;
}


/*
 * FakeReScanForeignScan restarts the fake plan (i.e. does nothing).
 */
static void
FakeReScanForeignScan(ForeignScanState *node) { }


/*
 * FakeEndForeignScan ends the fake plan (i.e. does nothing).
 */
static void
FakeEndForeignScan(ForeignScanState *node) { }
