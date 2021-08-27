/*-------------------------------------------------------------------------
 *
 * create_timeseries_table.c
 *	  Routines related to the creation of timeseries tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "access/genam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/resource_lock.h"
#include "timeseries/timeseries_utils.h"

#define default_premake_interval_count 7

PG_FUNCTION_INFO_V1(create_timeseries_table);

static void InitiateTimeseriesTablePartitions(Oid relationId, bool useStartFrom);
static void InsertIntoCitusTimeseriesTables(Oid relationId, Interval *partitionInterval,
											int postMakePartitionCount, int
											preMakePartitionCount,
											TimestampTz startFrom,
											Interval *compressionThresholdInterval,
											Interval *retentionThresholdInterval);
static void ErrorIfNotSuitableToConvertTimeseriesTable(Oid relationId,
													   Interval *partitionInterval,
													   Interval *
													   compresstionThresholdInterval,
													   Interval *
													   retentionThresholdInterval);

/*
 * create_timeseries_table gets a table name, partition interval
 * optional post and pre make interval count, start_from time,
 * compression and retention threshold then it creates a timeseries table.
 */
Datum
create_timeseries_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		ereport(ERROR, (errmsg("table name and partition interval "
							   "must be provided")));
		PG_RETURN_VOID();
	}

	if (!PG_ARGISNULL(3) && !PG_ARGISNULL(4))
	{
		ereport(ERROR, (errmsg("either premake_interval_count or start_from "
							   "should be provided")));

		PG_RETURN_VOID();
	}

	Oid relationId = PG_GETARG_OID(0);
	Interval *partitionInterval = PG_GETARG_INTERVAL_P(1);
	int postMakePartitionCount = PG_GETARG_INT32(2);
	int preMakePartitionCount = 0;
	TimestampTz startFrom = 0;
	Interval *compressionThresholdInterval = NULL;
	Interval *retentionThresholdInterval = NULL;
	bool useStartFrom = false;

	if (!PG_ARGISNULL(3))
	{
		preMakePartitionCount = DatumGetInt32(PG_GETARG_DATUM(3));
	}
	else if (!PG_ARGISNULL(4))
	{
		startFrom = DatumGetTimestampTz(PG_GETARG_DATUM(4));
		useStartFrom = true;
	}
	else
	{
		preMakePartitionCount = default_premake_interval_count;
	}


	if (!PG_ARGISNULL(5))
	{
		compressionThresholdInterval = DatumGetIntervalP(PG_GETARG_DATUM(5));
	}

	if (!PG_ARGISNULL(6))
	{
		retentionThresholdInterval = DatumGetIntervalP(PG_GETARG_DATUM(6));
	}

	ErrorIfNotSuitableToConvertTimeseriesTable(relationId, partitionInterval,
											   compressionThresholdInterval,
											   retentionThresholdInterval);

	InsertIntoCitusTimeseriesTables(relationId, partitionInterval, postMakePartitionCount,
									preMakePartitionCount, startFrom,
									compressionThresholdInterval,
									retentionThresholdInterval);

	InitiateTimeseriesTablePartitions(relationId, useStartFrom);

	PG_RETURN_VOID();
}


/*
 * Check whether the given table and intervals are suitable to convert a table to timeseries table,
 * if not error out.
 */
static void
ErrorIfNotSuitableToConvertTimeseriesTable(Oid relationId, Interval *partitionInterval,
										   Interval *compressionThresholdInterval,
										   Interval *retentionThresholdInterval)
{
	Relation pgPartitionedTableRelation = table_open(relationId, AccessShareLock);
	PartitionKey partitionKey = RelationGetPartitionKey(pgPartitionedTableRelation);

	/* Table related checks */
	if (!PartitionedTable(relationId) ||
		partitionKey->strategy != PARTITION_STRATEGY_RANGE)
	{
		ereport(ERROR, (errmsg("table must be partitioned by range to convert "
							   "it to timeseries table")));
	}

	if (partitionKey->partnatts != 1)
	{
		ereport(ERROR, (errmsg("table must be partitioned by single column to "
							   "convert it to timeseries table")));
	}

	if (!TableEmpty(relationId))
	{
		ereport(ERROR, (errmsg("table must be empty to convert it to "
							   "timeseries table")));
	}

	/* Timeseries parameter related checks */
	if (!CheckIntervalAlignmentWithThresholds(partitionInterval,
											  compressionThresholdInterval,
											  retentionThresholdInterval))
	{
		ereport(ERROR, (errmsg("retention threshold must be greater than "
							   "compression threshold and compression threshold "
							   "must be greater than partition interval")));
	}

	if (!CheckIntervalAlignnmentWithPartitionKey(partitionKey, partitionInterval))
	{
		ereport(ERROR, (errmsg("partition interval for table partitioned on "
							   "date must be multiple days")));
	}

	if (compressionThresholdInterval != NULL && !CheckIntervalAlignnmentWithPartitionKey(
			partitionKey, compressionThresholdInterval))
	{
		ereport(ERROR, (errmsg("compression threshold interval for table "
							   "partitioned on date must be multiple days")));
	}

	if (retentionThresholdInterval != NULL && !CheckIntervalAlignnmentWithPartitionKey(
			partitionKey, retentionThresholdInterval))
	{
		ereport(ERROR, (errmsg("retention threshold interval for table "
							   "partitioned on date must be multiple days")));
	}

	table_close(pgPartitionedTableRelation, NoLock);
}


/*
 * Create the initial pre and post make partitions for the given relation id
 * by getting the related information from timeseries.tables and utilizing
 * create_missing_partitions
 */
static void
InitiateTimeseriesTablePartitions(Oid relationId, bool useStartFrom)
{
	bool readOnly = false;
	StringInfo initiateTimeseriesPartitionsCommand = makeStringInfo();

	/*
	 * Since making the right calculation via C is much more complex and
	 * we will call that function once to create timeseries table, we
	 * are using SPI call.
	 */
	if (useStartFrom)
	{
		appendStringInfo(initiateTimeseriesPartitionsCommand,
						 "SELECT "
						 "pg_catalog.create_missing_partitions("
						 "logicalrelid,"
						 "now() + partitioninterval * postmakeintervalcount,"
						 "startfrom) "
						 "FROM timeseries.tables "
						 "WHERE logicalrelid = %d;",
						 relationId);
	}
	else
	{
		appendStringInfo(initiateTimeseriesPartitionsCommand,
						 "SELECT "
						 "pg_catalog.create_missing_partitions("
						 "logicalrelid,"
						 "now() + partitioninterval * postmakeintervalcount,"
						 "now() - partitioninterval * premakeintervalcount) "
						 "FROM timeseries.tables "
						 "WHERE logicalrelid = %d;",
						 relationId);
	}

	int spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("could not connect to SPI manager to "
								 "initiate timeseries table partitions")));
		SPI_finish();
	}

	SPI_execute(initiateTimeseriesPartitionsCommand->data, readOnly, 0);
	SPI_finish();
}


/*
 * Add tuples for the given table to the timeseries.tables using given params
 */
static void
InsertIntoCitusTimeseriesTables(Oid relationId, Interval *partitionInterval,
								int postMakePartitionCount, int preMakePartitionCount,
								TimestampTz startFrom,
								Interval *compressionThresholdInterval,
								Interval *retentionThresholdInterval)
{
	Datum newValues[Natts_citus_timeseries_tables];
	bool isNulls[Natts_citus_timeseries_tables];

	Relation citusTimeseriesTable = table_open(CitusTimeseriesTablesRelationId(),
											   RowExclusiveLock);

	memset(isNulls, false, sizeof(isNulls));

	newValues[0] = ObjectIdGetDatum(relationId);
	newValues[1] = IntervalPGetDatum(partitionInterval);
	newValues[2] = Int32GetDatum(postMakePartitionCount);
	newValues[3] = Int32GetDatum(preMakePartitionCount);

	if (startFrom != 0)
	{
		newValues[4] = TimestampTzGetDatum(startFrom);
	}
	else
	{
		isNulls[4] = true;
	}

	if (compressionThresholdInterval != NULL)
	{
		newValues[5] = IntervalPGetDatum(compressionThresholdInterval);
	}
	else
	{
		isNulls[5] = true;
	}

	if (retentionThresholdInterval != NULL)
	{
		newValues[6] = IntervalPGetDatum(retentionThresholdInterval);
	}
	else
	{
		isNulls[6] = true;
	}

	HeapTuple newTuple = heap_form_tuple(RelationGetDescr(citusTimeseriesTable),
										 newValues, isNulls);
	CatalogTupleInsert(citusTimeseriesTable, newTuple);

	table_close(citusTimeseriesTable, NoLock);
}
