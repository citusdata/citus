/*-------------------------------------------------------------------------
 *
 * create_timeseries_table.c
 *	  Routines related to the creation of timeseries relations.
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
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/resource_lock.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define Natts_citus_timeseries_tables 6

#define INTERVAL_TO_SEC(ivp) \
	(((double) (ivp)->time) / ((double) USECS_PER_SEC) + \
	 (ivp)->day * (24.0 * SECS_PER_HOUR) + \
	 (ivp)->month * (30.0 * SECS_PER_DAY))

PG_FUNCTION_INFO_V1(create_timeseries_table);

static Oid TimeseriesNamespaceId(void);
static Oid CitusTimeseriesTablesRelationId();
static void InitiateTimeseriesTablePartitions(Oid relationId);
static void InsertIntoCitusTimeseriesTables(Oid relationId, Interval *partitionInterval, int preMakePartitionCount,
											int postMakePartitionCount, Interval *compressionThresholdInterval,
											Interval *retentionThresholdInterval);
static void ErrorIfNotSuitableToConvertTimeseriesTable(Oid relationId, Interval *partitionInterval, 
													   Interval *compresstionThresholdInterval,
													   Interval *retentionThresholdInterval);
static bool CheckIntervalAlignnmentWithPartitionKey(PartitionKey partitionKey, Interval *partitionInterval);
static bool CheckIntervalAlignmentWithThresholds(Interval *partitionInterval, Interval *compressionThreshold, Interval *retentionThreshold);

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
		ereport(ERROR, (errmsg("must be retention threshold > compression threshold > partition interval")));
	}

	if (!CheckIntervalAlignnmentWithPartitionKey(partitionKey, partitionInterval))
	{
		ereport(ERROR, (errmsg("partition interval for partition on date must be multiple days")));
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
 * Compare partition interval, compression threshold and retenetion threshold. Note that
 * compression threshold or retention threshold can be null.
 */
static bool CheckIntervalAlignmentWithThresholds(Interval *partitionInterval, Interval *compressionThreshold, Interval *retentionThreshold)
{
	bool compressionGreaterThanInterval = compressionThreshold == NULL ? true : INTERVAL_TO_SEC(compressionThreshold) > INTERVAL_TO_SEC(partitionInterval);
	bool retentionGreaterThanInterval = retentionThreshold == NULL ? true : INTERVAL_TO_SEC(retentionThreshold) > INTERVAL_TO_SEC(partitionInterval);
	bool retentionGreaterThanCompression = compressionThreshold == NULL || retentionThreshold == NULL ? true : INTERVAL_TO_SEC(retentionThreshold) > INTERVAL_TO_SEC(compressionThreshold);

	return compressionGreaterThanInterval && retentionGreaterThanInterval && retentionGreaterThanCompression;
}

/*
 * Check whether the given partition interval aligns with the partition column of the table.
 */
static bool CheckIntervalAlignnmentWithPartitionKey(PartitionKey partitionKey, Interval *partitionInterval)
{
	Oid partTypeId;
	HeapTuple typeTuple;
	Form_pg_type typeForm;

	partTypeId = partitionKey->parttypid[0];
	typeTuple = SearchSysCache1(TYPEOID, partTypeId);
	typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
	ReleaseSysCache(typeTuple);

	if(strncmp(typeForm->typname.data, "date", NAMEDATALEN) == 0)
	{
		if (partitionInterval->time == 0)
		{
			return true;
		}
	}
	else if(strncmp(typeForm->typname.data, "timestamp", NAMEDATALEN) == 0 ||
			strncmp(typeForm->typname.data, "timestamptz", NAMEDATALEN) == 0)
	{
		return true;
	}

	return false;
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

/*
 * Get the relation id for citus_timeseries_tables metadata table
 */
static Oid
CitusTimeseriesTablesRelationId()
{
	Oid relationId = get_relname_relid("citus_timeseries_tables", TimeseriesNamespaceId());
	if (relationId == InvalidOid)
	{
		ereport(ERROR, (errmsg("cache lookup failed for citus_timeseries_tables, called too early?")));
	}

	return relationId;
}

/*
 * TimeseriesNamespaceId returns namespace id of the schema we store timeseries
 * related metadata tables.
 */
static Oid
TimeseriesNamespaceId(void)
{
	return get_namespace_oid("citus_timeseries", false);
}