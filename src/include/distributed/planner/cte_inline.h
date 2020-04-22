/*-------------------------------------------------------------------------
 *
 * cte_inline.h
 *	Functions and global variables to control cte inlining.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CTE_INLINE_H
#define CTE_INLINE_H

#include "nodes/parsenodes.h"

extern bool EnableCTEInlining;

extern void RecursivelyInlineCtesInQueryTree(Query *query);
extern bool QueryTreeContainsInlinableCTE(Query *queryTree);

#endif /* CTE_INLINE_H */
