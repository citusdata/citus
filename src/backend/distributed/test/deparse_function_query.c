/*-------------------------------------------------------------------------
 *
 * test/src/depase_function_query.c
 *
 * This file contains functions to exercise deparsing of
 *  CREATE|ALTER|DROP [...] function/procedure ...
 * statements
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(deparse_test);


Datum
deparse_test(PG_FUNCTION_ARGS)
{
	text *queryStringText = PG_GETARG_TEXT_P(0);

	char *queryStringChar = NULL;
	List *parseTreeList = NIL;
	ListCell *parseTreeCell = NULL;
	StringInfo string = makeStringInfo();

	queryStringChar = text_to_cstring(queryStringText);
	parseTreeList = pg_parse_query(queryStringChar);

	foreach(parseTreeCell, parseTreeList)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parseTreeCell);
		const char *deparsedQuery = NULL;

		/* TODO: Do I need to call pg_analyze_and_rewrite? Do I need it to change parsetree? */
		pg_analyze_and_rewrite(parsetree, queryStringChar, NULL, 0, NULL);

		deparsedQuery = DeparseTreeNode(parsetree->stmt);

		if (strcmp(string->data, "") != 0)
		{
			appendStringInfoString(string, "; ");
		}
		appendStringInfoString(string, deparsedQuery);
	}

	PG_RETURN_TEXT_P(cstring_to_text(string->data));
}
