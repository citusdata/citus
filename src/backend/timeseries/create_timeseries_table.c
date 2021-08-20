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

#define Natts_citus_timeseries_tables 6

PG_FUNCTION_INFO_V1(create_timeseries_table);

static void InitiateTimeseriesTablePartitions(Oid relationId);
static void InsertIntoCitusTimeseriesTables(Oid relationId, Interval *partitionInterval, int preMakePartitionCount,
											int postMakePartitionCount, Interval *compressionThresholdInterval,
											Interval *retentionThresholdInterval);
static void ErrorIfNotSuitableToConvertTimeseriesTable(Oid relationId, Interval *partitionInterval, 
													   Interval *compresstionThresholdInterval,
													   Interval *retentionThresholdInterval);

/*
 * create_timeseries_table gets a table name, partition interval 
 * optional pre and post make partition counts, compression and retention threshold
 * then it creates a timeseries table.
 */
Datum
create_timeseries_table(PG_FUNCTION_ARGS)
{
    CheckCitusVersion(ERROR);

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		PG_RETURN_VOID();
	}

	Oid relationId = PG_GETARG_OID(0);
	Interval *partitionInterval = PG_GETARG_INTERVAL_P(1);
    int preMakePartitionCount = PG_GETARG_INT32(2);
    int postMakePartitionCount = PG_GETARG_INT32(3);
	Interval *compressionThresholdInterval = NULL;
	Interval *retentionThresholdInterval = NULL;

	if (!PG_ARGISNULL(4))
	{
		compressionThresholdInterval = DatumGetIntervalP(PG_GETARG_DATUM(4));
	}

	if (!PG_ARGISNULL(5))
	{
		retentionThresholdInterval = DatumGetIntervalP(PG_GETARG_DATUM(5));
	}

	ErrorIfNotSuitableToConvertTimeseriesTable(relationId, partitionInterval, compressionThresholdInterval, retentionThresholdInterval);

	InsertIntoCitusTimeseriesTables(relationId, partitionInterval, preMakePartitionCount, 
                                    postMakePartitionCount, compressionThresholdInterval, retentionThresholdInterval);

	InitiateTimeseriesTablePartitions(relationId);

	PG_RETURN_VOID();
}

/*
 * Check whether the given table and intervals are suitable to convert a table to timeseries table,
 * if not error out.
 */
static void ErrorIfNotSuitableToConvertTimeseriesTable(Oid relationId, Interval *partitionInterval, Interval *compressionThresholdInterval, Interval *retentionThresholdInterval)
{
	Relation pgPartitionedTableRelation;
	PartitionKey partitionKey;

	if(!TableEmpty(relationId))
	{
		ereport(ERROR, (errmsg("non-empty tables can not be converted to timeseries table")));
	}

	if(!PartitionedTable(relationId))
	{
		ereport(ERROR, (errmsg("non-partitioned tables can not be converted to timeseries table")));
	}

	pgPartitionedTableRelation = table_open(relationId, AccessShareLock);
	partitionKey = RelationGetPartitionKey(pgPartitionedTableRelation);

	if (partitionKey->strategy != PARTITION_STRATEGY_RANGE)
	{
		ereport(ERROR, (errmsg("table must be partitioned by range to convert it to timeseries table")));
	}

	if (partitionKey->partnatts != 1)
	{
		ereport(ERROR, (errmsg("table must be partitioned by single column to convert it to timeseries table")));
	}

	if (!CheckIntervalAlignmentWithThresholds(partitionInterval, compressionThresholdInterval, retentionThresholdInterval))
	{
		ereport(ERROR, (errmsg("retention threshold must be greater than compression threshold and compresstion threshold must be greater than partition interval")));
	}

	if (!CheckIntervalAlignnmentWithPartitionKey(partitionKey, partitionInterval))
	{
		ereport(ERROR, (errmsg("partition interval for table partitioned on date must be multiple days")));
	}

	if (compressionThresholdInterval != NULL && !CheckIntervalAlignnmentWithPartitionKey(partitionKey, compressionThresholdInterval))
	{
		ereport(ERROR, (errmsg("compression threshold interval for table partitioned on date must be multiple days")));
	}

	if (retentionThresholdInterval != NULL && !CheckIntervalAlignnmentWithPartitionKey(partitionKey, retentionThresholdInterval))
	{
		ereport(ERROR, (errmsg("retention threshold interval for table partitioned on date must be multiple days")));
	}

	table_close(pgPartitionedTableRelation, NoLock);
}

/*
 * Create the initial pre and post make partitions for the given relation id
 * by getting the related information from citus_timeseries_tables and utilizing
 * create_missing_partitions
 */
static void
InitiateTimeseriesTablePartitions(Oid relationId)
{
	bool readOnly = false;
	StringInfo initiateTimeseriesPartitionsCommand = makeStringInfo();
	appendStringInfo(initiateTimeseriesPartitionsCommand, "SELECT create_missing_partitions(logicalrelid, now() + partitioninterval * postmakeintervalcount, now() - partitioninterval * premakeintervalcount) from citus_timeseries.citus_timeseries_tables WHERE logicalrelid = %d;", relationId); 

	int spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("could not connect to SPI manager to initiate timeseries table partitions")));
		SPI_finish();
	}

	SPI_execute(initiateTimeseriesPartitionsCommand->data, readOnly, 0);
	SPI_finish();
}

/*
 * Add tuples for the given table to the citus_timeseries_tables using given params
 */
static void
InsertIntoCitusTimeseriesTables(Oid relationId, Interval *partitionInterval, int preMakePartitionCount, int postMakePartitionCount,
                                Interval *compressionThresholdInterval, Interval *retentionThresholdInterval)
{
	Datum newValues[Natts_citus_timeseries_tables];
	bool isNulls[Natts_citus_timeseries_tables];

	Relation citusTimeseriesTable = table_open(CitusTimeseriesTablesRelationId(), RowExclusiveLock);

	memset(newValues, 0, sizeof(newValues));
	memset(isNulls, false, sizeof(isNulls));

	newValues[0] = ObjectIdGetDatum(relationId);
	newValues[1] = IntervalPGetDatum(partitionInterval);
    newValues[2] = Int32GetDatum(preMakePartitionCount);
    newValues[3] = Int32GetDatum(postMakePartitionCount);
	
	if (compressionThresholdInterval != NULL)
	{
		newValues[4] = IntervalPGetDatum(compressionThresholdInterval);
	}
	else
	{
		isNulls[4] = true;
	}

	if (retentionThresholdInterval != NULL)
	{
		newValues[5] = IntervalPGetDatum(retentionThresholdInterval);
	}
	else
	{
		isNulls[5] = true;
	}

	HeapTuple newTuple = heap_form_tuple(RelationGetDescr(citusTimeseriesTable), newValues, isNulls);
	CatalogTupleInsert(citusTimeseriesTable, newTuple);

	table_close(citusTimeseriesTable, NoLock);
}
