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

#include "pg_version_compat.h"


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
 * foreach_declared_ptr -
 *	  a convenience macro which loops through a pointer list without needing a
 *	  ListCell, just a declared pointer variable to store the pointer of the
 *	  cell in.
 *
 *   How it works:
 *	  - A ListCell is declared with the name {var}CellDoNotUse and used
 *	    throughout the for loop using ## to concat.
 *	  - To assign to var it needs to be done in the condition of the for loop,
 *	    because we cannot use the initializer since a ListCell* variable is
 *	    declared there.
 *	  - || true is used to always enter the loop when cell is not null even if
 *	    var is NULL.
 */
#define foreach_declared_ptr(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext(l, var ## CellDoNotUse))


/*
 * foreach_declared_int -
 *	  a convenience macro which loops through an int list without needing a
 *	  ListCell, just a declared int variable to store the int of the cell in.
 *	  For explanation of how it works see foreach_declared_ptr.
 */
#define foreach_declared_int(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst_int(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext(l, var ## CellDoNotUse))


/*
 * foreach_declared_oid -
 *	  a convenience macro which loops through an oid list without needing a
 *	  ListCell, just a declared Oid variable to store the oid of the cell in.
 *	  For explanation of how it works see foreach_declared_ptr.
 */
#define foreach_declared_oid(var, l) \
	for (ListCell *(var ## CellDoNotUse) = list_head(l); \
		 (var ## CellDoNotUse) != NULL && \
		 (((var) = lfirst_oid(var ## CellDoNotUse)) || true); \
		 var ## CellDoNotUse = lnext(l, var ## CellDoNotUse))

/*
 * forboth_ptr -
 *	  a convenience macro which loops through two lists of pointers at the same
 *	  time, without needing a ListCell. It only needs two declared pointer
 *	  variables to store the pointer of each of the two cells in.
 */
#define forboth_ptr(var1, l1, var2, l2) \
	for (ListCell *(var1 ## CellDoNotUse) = list_head(l1), \
		 *(var2 ## CellDoNotUse) = list_head(l2); \
		 (var1 ## CellDoNotUse) != NULL && \
		 (var2 ## CellDoNotUse) != NULL && \
		 (((var1) = lfirst(var1 ## CellDoNotUse)) || true) && \
		 (((var2) = lfirst(var2 ## CellDoNotUse)) || true); \
		 var1 ## CellDoNotUse = lnext(l1, var1 ## CellDoNotUse), \
		 var2 ## CellDoNotUse = lnext(l2, var2 ## CellDoNotUse) \
		 )

/*
 * forboth_ptr_oid -
 *	  a convenience macro which loops through two lists at the same time. The
 *	  first list should contain pointers and the second list should contain
 *	  Oids. It does not need a ListCell to do this. It only needs two declared
 *	  variables to store the pointer and the Oid of each of the two cells in.
 */
#define forboth_ptr_oid(var1, l1, var2, l2) \
	for (ListCell *(var1 ## CellDoNotUse) = list_head(l1), \
		 *(var2 ## CellDoNotUse) = list_head(l2); \
		 (var1 ## CellDoNotUse) != NULL && \
		 (var2 ## CellDoNotUse) != NULL && \
		 (((var1) = lfirst(var1 ## CellDoNotUse)) || true) && \
		 (((var2) = lfirst_oid(var2 ## CellDoNotUse)) || true); \
		 var1 ## CellDoNotUse = lnext(l1, var1 ## CellDoNotUse), \
		 var2 ## CellDoNotUse = lnext(l2, var2 ## CellDoNotUse) \
		 )

/*
 * forboth_int_oid -
 *	  a convenience macro which loops through two lists at the same time. The
 *	  first list should contain integers and the second list should contain
 *	  Oids. It does not need a ListCell to do this. It only needs two declared
 *	  variables to store the int and the Oid of each of the two cells in.
 */
#define forboth_int_oid(var1, l1, var2, l2) \
	for (ListCell *(var1 ## CellDoNotUse) = list_head(l1), \
		 *(var2 ## CellDoNotUse) = list_head(l2); \
		 (var1 ## CellDoNotUse) != NULL && \
		 (var2 ## CellDoNotUse) != NULL && \
		 (((var1) = lfirst_int(var1 ## CellDoNotUse)) || true) && \
		 (((var2) = lfirst_oid(var2 ## CellDoNotUse)) || true); \
		 var1 ## CellDoNotUse = lnext(l1, var1 ## CellDoNotUse), \
		 var2 ## CellDoNotUse = lnext(l2, var2 ## CellDoNotUse) \
		 )

/*
 * foreach_ptr_append -
 *	  a convenience macro which loops through a pointer List and can append list
 *	  elements without needing a ListCell or and index variable, just a declared
 *	  pointer variable to store the iterated values.
 *
 *	  PostgreSQL 13 changed the representation of Lists to expansible arrays,
 *	  not chains of cons-cells. This changes the costs for accessing and
 *	  mutating List contents. Therefore different implementations are provided.
 *
 *	  For more information, see postgres commit with sha
 *	  1cff1b95ab6ddae32faa3efe0d95a820dbfdc164
 *
 *	  How it works:
 *	  - An index is declared with the name {var}PositionDoNotUse and used
 *	    throughout the for loop using ## to concat.
 *	  - To assign to var it needs to be done in the condition of the for loop,
 *	    because we cannot use the initializer since the index variable is
 *	    declared there.
 *	  - || true is used to always enter the loop even if var is NULL.
 */
#define foreach_ptr_append(var, l) \
	for (int var ## PositionDoNotUse = 0; \
		 (var ## PositionDoNotUse) < list_length(l) && \
		 (((var) = list_nth(l, var ## PositionDoNotUse)) || true); \
		 var ## PositionDoNotUse ++)

/* utility functions declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));
extern void ** PointerArrayFromList(List *pointerList);
extern HTAB * ListToHashSet(List *pointerList, Size keySize, bool isStringList);
extern char * StringJoin(List *stringList, char delimiter);
extern char * StringJoinParams(List *stringList, char delimiter,
							   char *prefix, char *postfix);
extern List * ListTake(List *pointerList, int size);
extern void * safe_list_nth(const List *list, int index);
extern List * GeneratePositiveIntSequenceList(int upTo);
extern List * GenerateListFromElement(void *listElement, int listLength);
extern List * list_filter_oid(List *list, bool (*keepElement)(Oid element));

#endif /* CITUS_LISTUTILS_H */
