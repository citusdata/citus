/*-------------------------------------------------------------------------
 *
 * test/src/deparse_function_query.c
 *
 * This file contains functions to exercise deparsing of
 *  CREATE|ALTER|DROP [...] {FUNCTION|PROCEDURE} ...
 * queries
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"

#include "distributed/deparser.h"
#include "distributed/multi_executor.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(deparse_test);


/*
 * deparse_test UDF is a UDF to test deparsing in Citus.
 *
 * This function accepts a query string; parses, qualifies and then deparses it to create
 * a qualified query string.
 */
Datum
deparse_test(PG_FUNCTION_ARGS)
{
	text *queryStringText = PG_GETARG_TEXT_P(0);

	char *queryStringChar = text_to_cstring(queryStringText);
	Query *query = ParseQueryString(queryStringChar, NULL, 0);

	QualifyTreeNode(query->utilityStmt);
	const char *deparsedQuery = DeparseTreeNode(query->utilityStmt);

	PG_RETURN_TEXT_P(cstring_to_text(deparsedQuery));
}
