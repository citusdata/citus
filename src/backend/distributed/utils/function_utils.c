/*-------------------------------------------------------------------------
 *
 * function_utils.c
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) 2012-2019, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/function_utils.h"
#include "distributed/version_compat.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/regproc.h"

/*
 * FunctionOid searches for a function that has the given name and the given
 * number of arguments, and returns the corresponding function's oid. The
 * function reports error if the target function is not found, or it found more
 * matching instances.
 */
Oid
FunctionOid(const char *schemaName, const char *functionName, int argumentCount)
{
	return FunctionOidExtended(schemaName, functionName, argumentCount, false);
}


/*
 * FunctionOidExtended searches for a given function identified by schema,
 * functionName, and argumentCount. It reports error if the function is not
 * found or there are more than one match. If the missingOK parameter is set
 * and there are no matches, then the function returns InvalidOid.
 */
Oid
FunctionOidExtended(const char *schemaName, const char *functionName, int argumentCount,
					bool missingOK)
{
	FuncCandidateList functionList = NULL;
	Oid functionOid = InvalidOid;

	char *qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
	List *qualifiedFunctionNameList = stringToQualifiedNameList(qualifiedFunctionName);
	List *argumentList = NIL;
	const bool findVariadics = false;
	const bool findDefaults = false;

	functionList = FuncnameGetCandidates(qualifiedFunctionNameList, argumentCount,
										 argumentList, findVariadics,
										 findDefaults, true);

	if (functionList == NULL)
	{
		if (missingOK)
		{
			return InvalidOid;
		}

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
	LOCAL_FCINFO(fcinfo, 1);
	FmgrInfo flinfo;
	ReturnSetInfo *rsinfo = makeNode(ReturnSetInfo);
	EState *estate = CreateExecutorState();
	rsinfo->econtext = GetPerTupleExprContext(estate);
	rsinfo->allowedModes = SFRM_Materialize;

	fmgr_info(functionId, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, (Node *) rsinfo);

	fcSetArg(fcinfo, 0, argument);

	(*function)(fcinfo);

	return rsinfo;
}
