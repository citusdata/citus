/*-------------------------------------------------------------------------
 *
 * internals_monitor.c
 *   UDFs for monitoring internal data structures of Citus.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "distributed/connection_management.h"

PG_FUNCTION_INFO_V1(citus_connections_hash);
PG_FUNCTION_INFO_V1(citus_zombie_connections);
PG_FUNCTION_INFO_V1(dont_kill_multiconnection);
PG_FUNCTION_INFO_V1(cleanup_zombie_connections);

static void CreateMultiConnectionTuple(MultiConnection *connection, Datum **valuesTuple,
									   bool **nullsTuple);
static void SetupReturnSet(FunctionCallInfo fcinfo, List *values, List *nulls);
static Datum NextRecord(FunctionCallInfo fcinfo);
static Datum DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength);

Datum
citus_connections_hash(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		List *valuesTupleList = NIL;
		List *nullsTupleList = NIL;
		HASH_SEQ_STATUS status;
		ConnectionHashEntry *entry;

		hash_seq_init(&status, ConnectionHash);
		while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
		{
			dlist_iter iter;

			dlist_foreach(iter, entry->connections)
			{
				MultiConnection *connection =
					dlist_container(MultiConnection, connectionNode, iter.cur);
				Datum *valuesTuple;
				bool *nullsTuple;
				CreateMultiConnectionTuple(connection, &valuesTuple, &nullsTuple);
				valuesTupleList = lappend(valuesTupleList, valuesTuple);
				nullsTupleList = lappend(nullsTupleList, nullsTuple);
			}
		}

		SetupReturnSet(fcinfo, valuesTupleList, nullsTupleList);
	}

	return NextRecord(fcinfo);
}


Datum
citus_zombie_connections(PG_FUNCTION_ARGS)
{
	if (SRF_IS_FIRSTCALL())
	{
		List *valuesTupleList = NIL;
		List *nullsTupleList = NIL;
		ListCell *connectionCell = NULL;
		foreach(connectionCell, ZombieConnections)
		{
			MultiConnection *connection = lfirst(connectionCell);
			Datum *valuesTuple;
			bool *nullsTuple;
			CreateMultiConnectionTuple(connection, &valuesTuple, &nullsTuple);
			valuesTupleList = lappend(valuesTupleList, valuesTuple);
			nullsTupleList = lappend(nullsTupleList, nullsTuple);
		}

		SetupReturnSet(fcinfo, valuesTupleList, nullsTupleList);
	}

	return NextRecord(fcinfo);
}


Datum
dont_kill_multiconnection(PG_FUNCTION_ARGS)
{
	int socketId = PG_GETARG_INT32(0);
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;

	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		dlist_mutable_iter iter;

		dlist_foreach_modify(iter, entry->connections)
		{
			MultiConnection *connection =
				dlist_container(MultiConnection, connectionNode, iter.cur);
			if (PQsocket(connection->pgConn) == socketId)
			{
				connection->dontKill = true;
			}
		}
	}

	PG_RETURN_VOID();
}


Datum
cleanup_zombie_connections(PG_FUNCTION_ARGS)
{
	ListCell *connectionCell = NULL;
	foreach(connectionCell, ZombieConnections)
	{
		MultiConnection *connection = lfirst(connectionCell);
		ShutdownConnection(connection);
	}
	PG_RETURN_VOID();
}


static void
CreateMultiConnectionTuple(MultiConnection *connection, Datum **valuesTuple,
						   bool **nullsTuple)
{
	RemoteTransaction *xact = &connection->remoteTransaction;
	const char *remoteXactStateStr[] = {
		"INVALID", "STARTING", "STARTED", "PREPARING", "PREPARED",
		"1PC_ABORTING", "2PC_ABORTING", "ABORTED", "1PC_COMMITTING",
		"2PC_COMMITTING", "COMMITTED"
	};

	*valuesTuple = palloc0(13 * sizeof(Datum));
	*nullsTuple = palloc0(13 * sizeof(bool));

	(*valuesTuple)[0] = CStringGetTextDatum(connection->hostname);
	(*valuesTuple)[1] = Int32GetDatum(connection->port);
	(*valuesTuple)[2] = CStringGetTextDatum(connection->user);
	(*valuesTuple)[3] = CStringGetTextDatum(connection->database);
	(*valuesTuple)[4] = Int32GetDatum(PQsocket(connection->pgConn));
	(*valuesTuple)[5] = BoolGetDatum(connection->sessionLifespan);
	(*valuesTuple)[6] = BoolGetDatum(connection->claimedExclusively);
	(*valuesTuple)[7] = TimestampTzGetDatum(connection->connectionStart);
	(*valuesTuple)[8] = CStringGetTextDatum(
		remoteXactStateStr[xact->transactionState]);
	(*valuesTuple)[9] = BoolGetDatum(xact->transactionCritical);
	(*valuesTuple)[10] = BoolGetDatum(xact->transactionFailed);
	(*valuesTuple)[11] = CStringGetTextDatum(xact->preparedName);
	(*valuesTuple)[12] = BoolGetDatum(connection->dontKill);
}


static void
SetupReturnSet(FunctionCallInfo fcinfo, List *valuesTupleList, List *nullsTupleList)
{
	MemoryContext oldContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	int tupleCount = list_length(valuesTupleList);
	Datum **valuesCopy = NULL;
	bool **nullsCopy = NULL;
	ListCell *valuesCell, *nullsCell;
	int i = 0;

	FuncCallContext *functionContext = SRF_FIRSTCALL_INIT();

	elog(WARNING, "tupleCount: %d", tupleCount);

	/*
	 * Switch to multi_call_memory_ctx so the results calculated here
	 * stays for the next call.
	 */
	oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

	get_call_result_type(fcinfo, NULL, &tupleDescriptor);

	valuesCopy = palloc0(tupleCount * sizeof(Datum *));
	nullsCopy = palloc0(tupleCount * sizeof(bool *));
	forboth(valuesCell, valuesTupleList, nullsCell, nullsTupleList)
	{
		Datum *values = lfirst(valuesCell);
		bool *nulls = lfirst(nullsCell);
		int j;
		valuesCopy[i] = palloc0(tupleDescriptor->natts * sizeof(Datum));
		nullsCopy[i] = palloc0(tupleDescriptor->natts * sizeof(Datum));

		for (j = 0; j < tupleDescriptor->natts; j++)
		{
			nullsCopy[i][j] = nulls[j];
			if (!nulls[j])
			{
				valuesCopy[i][j] = DatumCopy(values[j],
											 tupleDescriptor->attrs[j]->attbyval,
											 tupleDescriptor->attrs[j]->attlen);
			}
		}
		i++;
	}

	/* save results for future use */
	functionContext->max_calls = tupleCount;
	functionContext->user_fctx = list_make2(valuesCopy, nullsCopy);
	functionContext->tuple_desc = tupleDescriptor;

	MemoryContextSwitchTo(oldContext);
}


static Datum
NextRecord(FunctionCallInfo fcinfo)
{
	FuncCallContext *functionContext = SRF_PERCALL_SETUP();

	if (functionContext->call_cntr < functionContext->max_calls)
	{
		int i = functionContext->call_cntr;
		TupleDesc tupleDescriptor = functionContext->tuple_desc;
		Datum **values = linitial(functionContext->user_fctx);
		bool **nulls = lsecond(functionContext->user_fctx);

		HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values[i], nulls[i]);
		SRF_RETURN_NEXT(functionContext, HeapTupleGetDatum(heapTuple));
	}
	else
	{
		SRF_RETURN_DONE(functionContext);
	}
}


/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	Datum datumCopy = 0;

	if (datumTypeByValue)
	{
		datumCopy = datum;
	}
	else
	{
		uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
		char *datumData = palloc0(datumLength);
		memcpy(datumData, DatumGetPointer(datum), datumLength);

		datumCopy = PointerGetDatum(datumData);
	}

	return datumCopy;
}
