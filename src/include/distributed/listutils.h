/*-------------------------------------------------------------------------
 *
 * listutils.h
 *
 * Declarations for public utility functions related to lists.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_LISTUTILS_H
#define CITUS_LISTUTILS_H

#include "postgres.h"
#include "c.h"

#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/hsearch.h"


/*
 * foreach_ptr -
 *	  a convenience macro which loops through a pointer list without needing a
 *	  ListCell, just a declared pointer variable to store the pointer of the
 *	  cell in.
 *
 *   How it works:
 *	  - A ListCell is declared with the name {var}Cell and used throughout the
 *	    for loop using ## to concat.
 *	  - To assign to var it needs to be done in the condition of the for loop,
 *	    because we cannot use the initializer since a ListCell* variable is
 *	    declared there.
 *	  - || true is used to always enter the loop when cell is not null even if
 *	    var is NULL.
 */
#define foreach_ptr(var, l) \
	for (ListCell *(var ## Cell) = list_head(l); \
		 (var ## Cell) != NULL && (((var) = lfirst(var ## Cell)) || true); \
		 var ## Cell = lnext(var ## Cell))


/*
 * foreach_int -
 *	  a convenience macro which loops through an int list without needing a
 *	  ListCell, just a declared int variable to store the int of the cell in.
 *	  For explanation of how it works see foreach_ptr.
 */
#define foreach_int(var, l) \
	for (ListCell *(var ## Cell) = list_head(l); \
		 (var ## Cell) != NULL && (((var) = lfirst_int(var ## Cell)) || true); \
		 var ## Cell = lnext(var ## Cell))


/*
 * foreach_oid -
 *	  a convenience macro which loops through an oid list without needing a
 *	  ListCell, just a declared Oid variable to store the oid of the cell in.
 *	  For explanation of how it works see foreach_ptr.
 */
#define foreach_oid(var, l) \
	for (ListCell *(var ## Cell) = list_head(l); \
		 (var ## Cell) != NULL && (((var) = lfirst_oid(var ## Cell)) || true); \
		 var ## Cell = lnext(var ## Cell))


/* utility functions declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
extern void ** PointerArrayFromList(List *pointerList);
extern ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);
extern HTAB * ListToHashSet(List *pointerList, Size keySize, bool isStringList);
extern char * StringJoin(List *stringList, char delimiter);
extern List * ListTake(List *pointerList, int size);

#endif /* CITUS_LISTUTILS_H */
