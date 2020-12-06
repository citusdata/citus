/*-------------------------------------------------------------------------
 *
 * foreign_key_relationship_query.c
 *
 * This file contains UDFs for getting foreign constraint relationship between
 * distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "utils/builtins.h"


/* these functions are only exported in the regression tests */
PG_FUNCTION_INFO_V1(get_foreign_key_to_reference_table_commands);

/*
 * get_foreign_key_to_reference_table_commands returns the list of commands
 * for creating foreign keys to reference tables.
 */
Datum
get_foreign_key_to_reference_table_commands(PG_FUNCTION_ARGS)
{
	FuncCallContext *functionContext = NULL;
	ListCell *commandsCell = NULL;

	CheckCitusVersion(ERROR);

	/* for the first we call this UDF, we need to populate the result to return set */
	if (SRF_IS_FIRSTCALL())
	{
		Oid relationId = PG_GETARG_OID(0);

		/* create a function context for cross-call persistence */
		functionContext = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		MemoryContext oldContext = MemoryContextSwitchTo(
			functionContext->multi_call_memory_ctx);

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		ShardInterval *firstShardInterval = cacheEntry->sortedShardIntervalArray[0];
		ListCellAndListWrapper *wrapper = palloc0(sizeof(ListCellAndListWrapper));
		List *commandsList =
			GetForeignConstraintCommandsToReferenceTable(firstShardInterval);

		commandsCell = list_head(commandsList);
		wrapper->list = commandsList;
		wrapper->listCell = commandsCell;
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
		char *command = (char *) lfirst(wrapper->listCell);
		text *commandText = cstring_to_text(command);

		wrapper->listCell = lnext_compat(wrapper->list, wrapper->listCell);

		SRF_RETURN_NEXT(functionContext, PointerGetDatum(commandText));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}
