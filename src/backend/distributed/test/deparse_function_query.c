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
#include "distributed/multi_executor.h"
#include "utils/builtins.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(deparse_test);


Datum
deparse_test(PG_FUNCTION_ARGS)
{
	text *queryStringText = PG_GETARG_TEXT_P(0);
	char *queryStringChar = NULL;
	Query *query = NULL;
	const char *deparsedQuery = NULL;

	queryStringChar = text_to_cstring(queryStringText);
	query = ParseQueryString(queryStringChar, NULL, 0);
	QualifyTreeNode(query->utilityStmt);
	deparsedQuery = DeparseTreeNode(query->utilityStmt);

	PG_RETURN_TEXT_P(cstring_to_text(deparsedQuery));
}
