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
/* copy & paste from Postgres source, moved into a function for readability */
extern bool PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType);

/* the following utility functions are copy & paste from PostgreSQL code */
extern void inline_cte(Query *mainQuery, CommonTableExpr *cte);

#endif /* CTE_INLINE_H */
