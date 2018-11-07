/*-------------------------------------------------------------------------
 *
 * function_utils.c
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/function_utils.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#if (PG_VERSION_NUM >= 100000)
#include "utils/regproc.h"
#endif

/*
 * FunctionOid looks for a function that has the given name and the given number
 * of arguments, and returns the corresponding function's oid.
 */
Oid
FunctionOid(const char *schemaName, const char *functionName, int argumentCount)
{
	FuncCandidateList functionList = NULL;
	Oid functionOid = InvalidOid;

	char *qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
	List *qualifiedFunctionNameList = stringToQualifiedNameList(qualifiedFunctionName);
	List *argumentList = NIL;
	const bool findVariadics = false;
	const bool findDefaults = false;
	const bool missingOK = true;

	functionList = FuncnameGetCandidates(qualifiedFunctionNameList, argumentCount,
										 argumentList, findVariadics,
										 findDefaults, missingOK);

	if (functionList == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("function \"%s\" does not exist", functionName)));
	}
	else if (functionList->next != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						errmsg("more than one function named \"%s\"", functionName)));
	}

	/* get function oid from function list's head */
	functionOid = functionList->oid;
	return functionOid;
}


/*
 * FunctionCallGetTupleStore1 calls the given set-returning PGFunction with the given
 * argument and returns the ResultSetInfo filled by the called function.
 */
ReturnSetInfo *
FunctionCallGetTupleStore1(PGFunction function, Oid functionId, Datum argument)
{
	FunctionCallInfoData fcinfo;
	FmgrInfo flinfo;
	ReturnSetInfo *rsinfo = makeNode(ReturnSetInfo);
	EState *estate = CreateExecutorState();
	rsinfo->econtext = GetPerTupleExprContext(estate);
	rsinfo->allowedModes = SFRM_Materialize;

	fmgr_info(functionId, &flinfo);
	InitFunctionCallInfoData(fcinfo, &flinfo, 1, InvalidOid, NULL, (Node *) rsinfo);

	fcinfo.arg[0] = argument;
	fcinfo.argnull[0] = false;

	(*function)(&fcinfo);

	return rsinfo;
}
