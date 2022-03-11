/*
 * cluster_clock.c
 *
 * Core funtion defintions to implement cluster clock.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include <sys/time.h>
#include "access/genam.h"
#include "commands/extension.h"
#include "storage/spin.h"

#include "distributed/cluster_clock.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/maintenanced.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_local_group.h"
#include "distributed/remote_commands.h"

PG_FUNCTION_INFO_V1(citus_get_cluster_clock);
PG_FUNCTION_INFO_V1(set_transaction_id_clock_value);

static void UpdateClockCatalog(void);


/*
 * GetEpochTimeMs returns the epoch value in milliseconds.
 */
uint64
GetEpochTimeMs(void)
{
	struct timeval tp;

	gettimeofday(&tp, NULL);

	uint64 result = (uint64) (tp.tv_sec) * 1000;
	result = result + (uint64) (tp.tv_usec) / 1000;
	return result;
}


/*
 * GetLocalClockValue returns logical_clock_value value stored in
 * the Citus catalog pg_dist_local_group in given "savedValue"
 * arg.
 *
 * Returns true on success, i.e.: finds a record in pg_dist_local_group,
 * and false otherwise.
 */
bool
GetLocalClockValue(uint64 *savedValue)
{
	ScanKey scanKey = NULL;
	int scanKeyCount = 0;
	bool success;

	Relation pgDistLocalGroupId = table_open(DistLocalGroupIdRelationId(),
											 AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistLocalGroupId,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistLocalGroupId);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(heapTuple))
	{
		Datum datumArray[Natts_pg_dist_local_group];
		bool isNullArray[Natts_pg_dist_local_group];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		if (isNullArray[Anum_pg_dist_local_logical_clock_value - 1])
		{
			ereport(ERROR, (errmsg("unexpected: got null clock value "
								   "stored in pg_dist_local_group")));
		}

		*savedValue = DatumGetInt64(
			datumArray[Anum_pg_dist_local_logical_clock_value - 1]);
		success = true;
	}
	else
	{
		/*
		 * Upgrade is happening. When upgrading postgres, pg_dist_local_group is
		 * temporarily empty before citus_finish_pg_upgrade() finishes execution.
		 */
		success = false;
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistLocalGroupId, AccessShareLock);

	return success;
}


/*
 * PersistLocalClockValue gets invoked in two places, one in Citus maintenance
 * daemon, and when a ever a backend exits, but that still leaves a time
 * window where the clock value used may not be current in the catalog.
 * TBD: May be store it during Checkpoint too?
 */
void
PersistLocalClockValue(int code, Datum argUnused)
{
	/* It's a no-op if catalogs are not fully baked yet */
	if (IsInitProcessingMode() || creating_extension ||
		!EnableVersionChecks || !IsMaintainanceDaemonProcess())
	{
		return;
	}

	StartTransactionCommand();

	if (LockCitusExtension() && CheckCitusVersion(LOG) && CitusHasBeenLoaded())
	{
		UpdateClockCatalog();
	}

	CommitTransactionCommand();
}


/*
 * UpdateClockCatalog persists the most recent logical clock value
 * into the catalog pg_dist_local_group.
 */
static void
UpdateClockCatalog(void)
{
	ScanKey scanKey = NULL;
	int scanKeyCount = 0;
	Datum values[Natts_pg_dist_local_group] = { 0 };
	bool isNull[Natts_pg_dist_local_group] = { false };
	bool replace[Natts_pg_dist_local_group] = { false };
	static uint64 savedClockValue = 0;

	uint64 clockValue = GetCurrentClusterClockValue();

	if (clockValue == savedClockValue)
	{
		/* clock didn't move, nothing to save */
		return;
	}

	/* clock should only move forward */
	Assert(clockValue > savedClockValue);

	Relation pgDistLocalGroupId = table_open(DistLocalGroupIdRelationId(),
											 RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistLocalGroupId);
	SysScanDesc scanDescriptor = systable_beginscan(pgDistLocalGroupId,
													InvalidOid,
													false,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(LOG, (errmsg("could not find valid entry in pg_dist_local")));
		systable_endscan(scanDescriptor);
		table_close(pgDistLocalGroupId, NoLock);
		return;
	}

	values[Anum_pg_dist_local_logical_clock_value - 1] = Int64GetDatum(clockValue);
	isNull[Anum_pg_dist_local_logical_clock_value - 1] = false;
	replace[Anum_pg_dist_local_logical_clock_value - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isNull, replace);

	CatalogTupleUpdate(pgDistLocalGroupId, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(DistLocalGroupIdRelationId());
	systable_endscan(scanDescriptor);
	table_close(pgDistLocalGroupId, RowExclusiveLock);

	/* cache the saved value */
	savedClockValue = clockValue;
}


/*
 * SetTransactionClusterClock() takes the connection list of participating nodes in
 * the current transaction, and polls the logical clock value of all the nodes. It
 * sets the maximum logical clock value of all the nodes in the distributed transaction
 * id, which may be used as commit order for individual objects.
 */
void
SetTransactionClusterClock(List *connectionList)
{
	/* get clock value of the local node */
	Datum value = GetNextClusterClock();
	uint64 globalClockValue = DatumGetUInt64(value);

	ereport(DEBUG1, (errmsg("Coordinator transaction clock %lu",
							globalClockValue)));

	/* get clock value from each node */
	MultiConnection *connection = NULL;
	StringInfo queryToSend = makeStringInfo();
	appendStringInfo(queryToSend, "SELECT citus_get_cluster_clock();");

	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, queryToSend->data);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* fetch the results and pick the maximum clock value of all the nodes */
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupts = true;

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ereport(ERROR,
					(errmsg("Internal error, connection failure")));
		}

		int64 rowCount = PQntuples(result);
		int64 colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != 1 || rowCount != 1)
		{
			ereport(ERROR,
					(errmsg("unexpected result from citus_get_cluster_clock()")));
		}

		value = ParseIntField(result, 0, 0);
		uint64 nodeClockValue = DatumGetUInt64(value);
		ereport(DEBUG1, (errmsg("Node(%lu) transaction clock %lu",
								connection->connectionId, nodeClockValue)));

		if (nodeClockValue > globalClockValue)
		{
			globalClockValue = nodeClockValue;
		}

		PQclear(result);
		ForgetResults(connection);
	}

	ereport(DEBUG1,
			(errmsg("Final global transaction clock %lu", globalClockValue)));

	/* Set the adjusted value locally */
	SetTransactionIdClockValue(globalClockValue);

	/* Set the clock value on participating worker nodes */
	resetStringInfo(queryToSend);
	appendStringInfo(queryToSend,
					 "SELECT citus_internal.set_transaction_id_clock_value(%lu);",
					 globalClockValue);

	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, queryToSend->data);

		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* Process the result */
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupts = true;

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);

		if (!IsResponseOK(result))
		{
			ereport(ERROR,
					(errmsg("Internal error, connection failure")));
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


/*
 * citus_get_cluster_clock() is an UDF that returns a monotonically increasing
 * logical clock. Clock guarantees to never go back in value after restarts, and
 * makes best attempt to keep the value close to unix epoch time in milliseconds.
 */
Datum
citus_get_cluster_clock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	uint64 nextClusterClockValue = GetNextClusterClock();

	PG_RETURN_UINT64(nextClusterClockValue);
}


/*
 * set_transaction_id_clock_value() is an internal UDF to set the transaction
 * clock value of the remote nodes' distributed transaction id.
 */
Datum
set_transaction_id_clock_value(PG_FUNCTION_ARGS)
{
	uint64 transactionClockValue = PG_GETARG_INT64(0);

	SetTransactionIdClockValue(transactionClockValue);

	PG_RETURN_VOID();
}
