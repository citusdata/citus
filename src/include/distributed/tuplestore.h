/*-------------------------------------------------------------------------
 *
 * tuplestore.h
 *	  Utilities regarding calls to PG functions
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */


#ifndef CITUS_TUPLESTORE_H
#define CITUS_TUPLESTORE_H
#include "funcapi.h"

/* Function declaration for getting oid for the given function name */
extern
ReturnSetInfo * CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc);

extern
Tuplestorestate * SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupdesc);
#endif
