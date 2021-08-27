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

/*
 * Get the relation id for timeseries.tables metadata table
 */
Oid
CitusTimeseriesTablesRelationId()
{
	Oid relationId = get_relname_relid("tables",
									   TimeseriesNamespaceId());
	if (relationId == InvalidOid)
	{
		ereport(ERROR, (errmsg("cache lookup failed for timeseries tables,"
							   "called too early?")));
	}

	return relationId;
}


/*
 * CitusTimeseriesTablesPKeyIndexRelationId returns relation id of timeseries.tables_pkey.
 */
Oid
CitusTimeseriesTablesPKeyIndexRelationId()
{
	return get_relname_relid("tables_pkey", TimeseriesNamespaceId());
}


/*
 * TimeseriesNamespaceId returns namespace id of the schema we store timeseries
 * related metadata tables.
 */
Oid
TimeseriesNamespaceId()
{
	return get_namespace_oid("timeseries", false);
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
	Oid partTypeId = partitionKey->parttypid[0];
	HeapTuple typeTuple = SearchSysCache1(TYPEOID, partTypeId);
	Form_pg_type typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
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
