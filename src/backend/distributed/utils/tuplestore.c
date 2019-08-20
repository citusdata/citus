/*-------------------------------------------------------------------------
 *
 * tuplestore.c
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "distributed/tuplestore.h"
#include "miscadmin.h"

/*
 * CheckTuplestoreReturn checks if a tuplestore can be returned in the callsite
 * of the UDF.
 */
ReturnSetInfo *
CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	/* IMPORTANT can this be undefined behaviour. Casting to ReturnSetInfo before
	 * we checked the type with IsA?
	 * In case the type actual type is more strictly aligned than ReturnSetInfo
	 * it is it seems:
	 * https://wiki.sei.cmu.edu/confluence/display/c/EXP36-C.+Do+not+cast+pointers+into+more+strictly+aligned+pointer+types */
	ReturnSetInfo *returnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* check to see if caller supports us returning a tuplestore */
	if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot " \
						"accept a set")));
	}
	if (!(returnSetInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}
	if (get_call_result_type(fcinfo, NULL, tupdesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}
	return returnSetInfo;
}


/*
 * SetupTuplestore sets up a tuplestore for returning data.
 */
Tuplestorestate *
SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupleDescriptor)
{
	ReturnSetInfo *resultSet = CheckTuplestoreReturn(fcinfo, tupleDescriptor);

	MemoryContext perQueryContext = resultSet->econtext->ecxt_per_query_memory;

	MemoryContext currentContext = MemoryContextSwitchTo(perQueryContext);
	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	resultSet->returnMode = SFRM_Materialize;
	resultSet->setResult = tupstore;
	resultSet->setDesc = *tupleDescriptor;
	MemoryContextSwitchTo(currentContext);

	return tupstore;
}
