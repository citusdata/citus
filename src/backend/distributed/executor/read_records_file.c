/*-------------------------------------------------------------------------
 *
 * read_records_file.c
 *
 *
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static bool IsBinaryFormat(char *copyFormatString);
static void ReadFileIntoTupleStore(char *fileName, bool isBinaryFormat, TupleDesc
								   tupleDescriptor,
								   EState *executorState, Tuplestorestate *tupstore);
static Relation StubRelation(TupleDesc tupleDescriptor);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(read_records_file);


Datum
read_records_file(PG_FUNCTION_ARGS)
{
	text *fileNameText = PG_GETARG_TEXT_P(0);
	char *fileNameString = text_to_cstring(fileNameText);
	text *copyFormatText = PG_GETARG_TEXT_P(1);
	char *copyFormatString = text_to_cstring(copyFormatText);
	bool isBinaryFormat = IsBinaryFormat(copyFormatString);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore = NULL;
	TupleDesc tupleDescriptor = NULL;
	EState *executorState = NULL;
	MemoryContext oldcontext = NULL;

	CheckCitusVersion(ERROR);
	EnsureSuperUser();

	/* check to see if query supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}

	if (!(rsinfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "materialize mode required, but it is not allowed in this context")));
	}

	/* get a tuple descriptor for our result type */
	switch (get_call_result_type(fcinfo, NULL, &tupleDescriptor))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
		}
		break;

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
		}
		break;

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
		}
		break;
	}

	tupleDescriptor = CreateTupleDescCopy(tupleDescriptor);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	MemoryContextSwitchTo(oldcontext);

	executorState = CreateExecutorState();

	ReadFileIntoTupleStore(fileNameString, isBinaryFormat, tupleDescriptor, executorState,
						   tupstore);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupleDescriptor;

	return (Datum) 0;
}


static bool
IsBinaryFormat(char *copyFormatString)
{
	bool isBinaryFormat = false;

	if (strncmp(copyFormatString, "binary", NAMEDATALEN) == 0)
	{
		isBinaryFormat = true;
	}

	return isBinaryFormat;
}


/*
 * static bool
 * IsBinaryFormat(Oid copyFormatOid)
 * {
 *  HeapTuple enumTuple = NULL;
 *  Form_pg_enum enumForm = NULL;
 *  const char *enumLabel = NULL;
 *  bool isBinaryFormat = false;
 *
 *  enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(copyFormatOid));
 *  if (!HeapTupleIsValid(enumTuple))
 *  {
 *      ereport(ERROR, (errmsg("invalid internal value for enum: %u",
 *                             copyFormatOid)));
 *  }
 *
 *  enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
 *  enumLabel = NameStr(enumForm->enumlabel);
 *
 *  if (strncmp(enumLabel, "binary", NAMEDATALEN) == 0)
 *  {
 *      isBinaryFormat = true;
 *  }
 *
 *  ReleaseSysCache(enumTuple);
 *
 *  return isBinaryFormat;
 * }
 */
static void
ReadFileIntoTupleStore(char *fileName, bool isBinaryFormat, TupleDesc tupleDescriptor,
					   EState *executorState, Tuplestorestate *tupstore)
{
	CopyState copyState = NULL;

	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);
	Relation stubRelation = StubRelation(tupleDescriptor);

	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));
	List *copyOptions = NIL;

	if (isBinaryFormat)
	{
		DefElem *copyOption = NULL;

#if (PG_VERSION_NUM >= 100000)
		int location = -1; /* "unknown" token location */
		copyOption = makeDefElem("format", (Node *) makeString("binary"), location);
#else
		copyOption = makeDefElem("format", (Node *) makeString("binary"));
#endif

		copyOptions = lappend(copyOptions, copyOption);
	}

#if (PG_VERSION_NUM >= 100000)
	copyState = BeginCopyFrom(NULL, stubRelation, fileName, false, NULL,
							  NULL, copyOptions);
#else
	copyState = BeginCopyFrom(stubRelation, fileName, false, NULL,
							  copyOptions);
#endif

	while (true)
	{
		MemoryContext oldContext = NULL;
		bool nextRowFound = false;

		ResetPerTupleExprContext(executorState);
		oldContext = MemoryContextSwitchTo(executorTupleContext);

		nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
									columnValues, columnNulls, NULL);
		if (!nextRowFound)
		{
			MemoryContextSwitchTo(oldContext);
			break;
		}

		tuplestore_putvalues(tupstore, tupleDescriptor, columnValues, columnNulls);
		MemoryContextSwitchTo(oldContext);
	}

	EndCopyFrom(copyState);
}


/*
 * StubRelation creates a stub Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there is no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
StubRelation(TupleDesc tupleDescriptor)
{
	Relation stubRelation = palloc0(sizeof(RelationData));
	stubRelation->rd_att = tupleDescriptor;
	stubRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	stubRelation->rd_rel->relkind = RELKIND_RELATION;

	return stubRelation;
}
