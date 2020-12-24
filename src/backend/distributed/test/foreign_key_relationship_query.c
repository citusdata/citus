/*-------------------------------------------------------------------------
 *
 * foreign_key_relationship_query.c
 *
 * This file contains UDFs for getting foreign constraint relationship between
 * distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/tuplestore.h"
#include "distributed/version_compat.h"


#define GET_FKEY_CONNECTED_RELATIONS_COLUMNS 1


/* these functions are only exported in the regression tests */
PG_FUNCTION_INFO_V1(get_referencing_relation_id_list);
PG_FUNCTION_INFO_V1(get_referenced_relation_id_list);
PG_FUNCTION_INFO_V1(get_foreign_key_connected_relations);

/*
 * get_referencing_relation_id_list returns the list of table oids that is referencing
 * by given oid recursively. It uses the list cached in the distributed table cache
 * entry.
 */
Datum
get_referencing_relation_id_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *foreignRelationCell = NULL;

	CheckCitusVersion(ERROR);

	/* for the first we call this UDF, we need to populate the result to return set */
	if (SRF_IS_FIRSTCALL())
	{
		Oid relationId = PG_GETARG_OID(0);
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		MemoryContext oldContext =
			MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
		List *refList = list_copy(
			cacheEntry->referencingRelationsViaForeignKey);
		ListCellAndListWrapper *wrapper = palloc0(sizeof(ListCellAndListWrapper));
		foreignRelationCell = list_head(refList);
		wrapper->list = refList;
		wrapper->listCell = foreignRelationCell;
		functionContext->user_fctx = wrapper;
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	ListCellAndListWrapper *wrapper =
		(ListCellAndListWrapper *) functionContext->user_fctx;
	if (wrapper->listCell != NULL)
	{
		Oid refId = lfirst_oid(wrapper->listCell);

		wrapper->listCell = lnext_compat(wrapper->list, wrapper->listCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(refId));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * get_referenced_relation_id_list returns the list of table oids that is referenced
 * by given oid recursively. It uses the list cached in the distributed table cache
 * entry.
 */
Datum
get_referenced_relation_id_list(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *foreignRelationCell = NULL;

	CheckCitusVersion(ERROR);

	/* for the first we call this UDF, we need to populate the result to return set */
	if (SRF_IS_FIRSTCALL())
	{
		Oid relationId = PG_GETARG_OID(0);
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		MemoryContext oldContext =
			MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
		List *refList = list_copy(cacheEntry->referencedRelationsViaForeignKey);
		foreignRelationCell = list_head(refList);
		ListCellAndListWrapper *wrapper = palloc0(sizeof(ListCellAndListWrapper));
		wrapper->list = refList;
		wrapper->listCell = foreignRelationCell;
		functionContext->user_fctx = wrapper;
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * On every call to this function, we get the current position in the
	 * statement list. We then iterate to the next position in the list and
	 * return the current statement, if we have not yet reached the end of
	 * list.
	 */
	functionContext = SRF_PERCALL_SETUP();

	ListCellAndListWrapper *wrapper =
		(ListCellAndListWrapper *) functionContext->user_fctx;

	if (wrapper->listCell != NULL)
	{
		Oid refId = lfirst_oid(wrapper->listCell);

		wrapper->listCell = lnext_compat(wrapper->list, wrapper->listCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(refId));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/*
 * get_foreign_key_connected_relations takes a relation, and returns relations
 * that are connected to input relation via a foreign key graph.
 */
Datum
get_foreign_key_connected_relations(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	Oid connectedRelationId;
	List *fkeyConnectedRelationIdList = GetForeignKeyConnectedRelationIdList(relationId);
	foreach_oid(connectedRelationId, fkeyConnectedRelationIdList)
	{
		Datum values[GET_FKEY_CONNECTED_RELATIONS_COLUMNS];
		bool nulls[GET_FKEY_CONNECTED_RELATIONS_COLUMNS];

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));

		values[0] = ObjectIdGetDatum(connectedRelationId);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, nulls);
	}

	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}
