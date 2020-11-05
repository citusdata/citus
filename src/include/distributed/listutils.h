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
#include "distributed/version_compat.h"


/*
 * ListCellAndListWrapper stores a list and list cell.
 * This struct is used for functionContext. When iterating a list
 * in separate function calls, we need both the list and the current cell.
 * Therefore this wrapper stores both of them.
 */
typedef struct ListCellAndListWrapper
{
	List *list;
	ListCell *listCell;
} ListCellAndListWrapper;

/*
 * foreach_ptr -
 *	  a convenience macro which loops through a pointer List without needing a
 *	  ListCell or and index variable, just a declared pointer variable to store
 *    the iterated values.
 *
 *    PostgreSQL 13 changed the representation of Lists to expansible arrays,
 *    not chains of cons-cells. This changes the costs for accessing and
 *    mutating List contents. Therefore different implementations are provided.
 *
 *    For more information, see postgres commit with sha
 *    1cff1b95ab6ddae32faa3efe0d95a820dbfdc164
 */

#if PG_VERSION_NUM >= PG_VERSION_13

/*
 *    How it works:
 *	  - An index is declared with the name {var}PositionDoNotUse and used
 *	    throughout the for loop using ## to concat.
 *	  - To assign to var it needs to be done in the condition of the for loop,
 *	    because we cannot use the initializer since the index variable is
 *	    declared there.
 *	  - || true is used to always enter the loop even if var is NULL.
 */
#define foreach_ptr(var, l) \
	for (int var ## PositionDoNotUse = 0; \
		 (var ## PositionDoNotUse) < list_length(l) && \
		 (((var) = list_nth(l, var ## PositionDoNotUse)) || true); \
		 var ## PositionDoNotUse++)
#else

/*
 *    How it works:
 *	  - A ListCell is declared with the name {var}CellDoNotUse and used
 *	    throughout the for loop using ## to concat.
 *	  - To assign to var it needs to be done in the condition of the for loop,
 *	    because we cannot use the initializer since a ListCell* variable is
 *	    declared there.
 *	  - || true is used to always enter the loop when cell is not null even if
 *	    var is NULL.
 */
#define foreach_ptr(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext_compat(l, var ## CellDoNotUse))
#endif

/*
 * foreach_int -
 *	  a convenience macro which loops through an int list without needing a
 *	  ListCell or an index variable, just a declared int variable to store the
 *	  iterated values.
 *	  For explanation of how it works see foreach_ptr.
 */
#if PG_VERSION_NUM >= PG_VERSION_13
#define foreach_int(var, l) \
	for (int var ## PositionDoNotUse = 0; \
		 (var ## PositionDoNotUse) < list_length(l) && \
		 (((var) = list_nth_int(l, var ## PositionDoNotUse)) || true); \
		 var ## PositionDoNotUse++)
#else
#define foreach_int(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst_int(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext_compat(l, var ## CellDoNotUse))
#endif

/*
 * foreach_oid -
 *	  a convenience macro which loops through an oid list without needing a
 *	  ListCell or an index variable, just a declared oid variable to store the
 *	  iterated values.
 *	  For explanation of how it works see foreach_ptr.
 */
#if PG_VERSION_NUM >= PG_VERSION_13
#define foreach_oid(var, l) \
	for (int var ## PositionDoNotUse = 0; \
		 (var ## PositionDoNotUse) < list_length(l) && \
		 (((var) = list_nth_oid(l, var ## PositionDoNotUse)) || true); \
		 var ## PositionDoNotUse++)
#else
#define foreach_oid(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst_oid(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext_compat(l, var ## CellDoNotUse))
#endif

/* utility functions declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
extern void ** PointerArrayFromList(List *pointerList);
extern ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);
extern HTAB * ListToHashSet(List *pointerList, Size keySize, bool isStringList);
extern char * StringJoin(List *stringList, char delimiter);
extern List * ListTake(List *pointerList, int size);
extern void * safe_list_nth(const List *list, int index);
extern List * GeneratePositiveIntSequenceList(int upTo);
extern List * GenerateListFromElement(void *listElement, int listLength);

#endif /* CITUS_LISTUTILS_H */
