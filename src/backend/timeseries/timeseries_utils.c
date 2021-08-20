/*-------------------------------------------------------------------------
 *
 * timeseries_utils.c
 *
 * This file contains utility functions for timeseries tables
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "datatype/timestamp.h"
#include "partitioning/partdefs.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/syscache.h"

#include "timeseries/timeseries_utils.h"

#define INTERVAL_TO_SEC(ivp) \
	(((double) (ivp)->time) / ((double) USECS_PER_SEC) + \
	 (ivp)->day * (24.0 * SECS_PER_HOUR) + \
	 (ivp)->month * (30.0 * SECS_PER_DAY))

/*
 * Get the relation id for citus_timeseries_tables metadata table
 */
Oid
CitusTimeseriesTablesRelationId()
{
	Oid relationId = get_relname_relid("citus_timeseries_tables",
									   TimeseriesNamespaceId());
	if (relationId == InvalidOid)
	{
		ereport(ERROR, (errmsg(
							"cache lookup failed for citus_timeseries_tables, called too early?")));
	}

	return relationId;
}


/*
 * TimeseriesNamespaceId returns namespace id of the schema we store timeseries
 * related metadata tables.
 */
Oid
TimeseriesNamespaceId()
{
	return get_namespace_oid("citus_timeseries", false);
}


/*
 * Compare partition interval, compression threshold and retenetion threshold. Note that
 * compression threshold or retention threshold can be null.
 */
bool
CheckIntervalAlignmentWithThresholds(Interval *partitionInterval,
									 Interval *compressionThreshold,
									 Interval *retentionThreshold)
{
	bool compressionGreaterThanInterval = compressionThreshold == NULL ? true :
										  INTERVAL_TO_SEC(compressionThreshold) >
										  INTERVAL_TO_SEC(partitionInterval);
	bool retentionGreaterThanInterval = retentionThreshold == NULL ? true :
										INTERVAL_TO_SEC(retentionThreshold) >
										INTERVAL_TO_SEC(partitionInterval);
	bool retentionGreaterThanCompression = compressionThreshold == NULL ||
										   retentionThreshold == NULL ? true :
										   INTERVAL_TO_SEC(retentionThreshold) >
										   INTERVAL_TO_SEC(compressionThreshold);

	return compressionGreaterThanInterval && retentionGreaterThanInterval &&
		   retentionGreaterThanCompression;
}


/*
 * Check whether the given partition interval aligns with the partition column of the table.
 */
bool
CheckIntervalAlignnmentWithPartitionKey(PartitionKey partitionKey,
										Interval *partitionInterval)
{
	Oid partTypeId;
	HeapTuple typeTuple;
	Form_pg_type typeForm;

	partTypeId = partitionKey->parttypid[0];
	typeTuple = SearchSysCache1(TYPEOID, partTypeId);
	typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
	ReleaseSysCache(typeTuple);

	if (strncmp(typeForm->typname.data, "date", NAMEDATALEN) == 0)
	{
		if (partitionInterval->time == 0)
		{
			return true;
		}
	}
	else if (strncmp(typeForm->typname.data, "timestamp", NAMEDATALEN) == 0 ||
			 strncmp(typeForm->typname.data, "timestamptz", NAMEDATALEN) == 0)
	{
		return true;
	}

	return false;
}
