/*-------------------------------------------------------------------------
 *
 * test/src/depase_shard_query.c
 *
 * This file contains functions to exercise deparsing of INSERT .. SELECT queries
 * for distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include <stddef.h>

#include "catalog/pg_type.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_router_planner.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/palloc.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(deparse_shard_query_test);


Datum
deparse_shard_query_test(PG_FUNCTION_ARGS)
{
	text *queryString = PG_GETARG_TEXT_P(0);

	char *queryStringChar = text_to_cstring(queryString);
	List *parseTreeList = pg_parse_query(queryStringChar);

	Node *parsetree = NULL;
	foreach_ptr(parsetree, parseTreeList)
	{
		List *queryTreeList = pg_analyze_and_rewrite((RawStmt *) parsetree,
													 queryStringChar,
													 NULL, 0, NULL);

		Query *query = NULL;
		foreach_ptr(query, queryTreeList)
		{
			StringInfo buffer = makeStringInfo();

			/* reoreder the target list only for INSERT .. SELECT queries */
			if (InsertSelectIntoCitusTable(query))
			{
				RangeTblEntry *insertRte = linitial(query->rtable);
				RangeTblEntry *subqueryRte = lsecond(query->rtable);


				ReorderInsertSelectTargetLists(query, insertRte, subqueryRte);
			}

			deparse_shard_query(query, InvalidOid, 0, buffer);

			elog(INFO, "query: %s", buffer->data);
		}
	}

	PG_RETURN_VOID();
}
