/*-------------------------------------------------------------------------
 *
 * cdc_decoder_utils.c
 *		CDC Decoder plugin utility functions for Citus
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "common/hashfn.h"
#include "common/string.h"
#include "utils/fmgroids.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_namespace.h"
#include "cdc_decoder_utils.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/relay_utility.h"

static int32 LocalGroupId = -1;
static Oid PgDistLocalGroupRelationId = InvalidOid;
static Oid PgDistShardRelationId = InvalidOid;
static Oid PgDistShardShardidIndexId = InvalidOid;
static Oid PgDistPartitionRelationId = InvalidOid;
static Oid PgDistPartitionLogicalrelidIndexId = InvalidOid;
static bool IsCitusExtensionLoaded = false;

#define COORDINATOR_GROUP_ID 0
#define InvalidRepOriginId 0
#define Anum_pg_dist_local_groupid 1
#define GROUP_ID_UPGRADING -2


static Oid DistLocalGroupIdRelationId(void);
static int32 CdcGetLocalGroupId(void);
static HeapTuple CdcPgDistPartitionTupleViaCatalog(Oid relationId);

/*
 * DistLocalGroupIdRelationId returns the relation id of the pg_dist_local_group
 */
static Oid
DistLocalGroupIdRelationId(void)
{
	if (PgDistLocalGroupRelationId == InvalidOid)
	{
		PgDistLocalGroupRelationId = get_relname_relid("pg_dist_local_group",
													   PG_CATALOG_NAMESPACE);
	}
	return PgDistLocalGroupRelationId;
}


/*
 * DistShardRelationId returns the relation id of the pg_dist_shard
 */
static Oid
DistShardRelationId(void)
{
	if (PgDistShardRelationId == InvalidOid)
	{
		PgDistShardRelationId = get_relname_relid("pg_dist_shard", PG_CATALOG_NAMESPACE);
	}
	return PgDistShardRelationId;
}


/*
 * DistShardRelationId returns the relation id of the pg_dist_shard
 */
static Oid
DistShardShardidIndexId(void)
{
	if (PgDistShardShardidIndexId == InvalidOid)
	{
		PgDistShardShardidIndexId = get_relname_relid("pg_dist_shard_shardid_index",
													  PG_CATALOG_NAMESPACE);
	}
	return PgDistShardShardidIndexId;
}


/*
 * DistShardRelationId returns the relation id of the pg_dist_shard
 */
static Oid
DistPartitionRelationId(void)
{
	if (PgDistPartitionRelationId == InvalidOid)
	{
		PgDistPartitionRelationId = get_relname_relid("pg_dist_partition",
													  PG_CATALOG_NAMESPACE);
	}
	return PgDistPartitionRelationId;
}


static Oid
DistPartitionLogicalRelidIndexId(void)
{
	if (PgDistPartitionLogicalrelidIndexId == InvalidOid)
	{
		PgDistPartitionLogicalrelidIndexId = get_relname_relid(
			"pg_dist_partition_logicalrelid_index", PG_CATALOG_NAMESPACE);
	}
	return PgDistPartitionLogicalrelidIndexId;
}


/*
 * CdcIsCoordinator function returns true if this node is identified as the
 * schema/coordinator/master node of the cluster.
 */
bool
CdcIsCoordinator(void)
{
	return (CdcGetLocalGroupId() == COORDINATOR_GROUP_ID);
}


/*
 * CdcCitusHasBeenLoaded function returns true if the citus extension has been loaded.
 */
bool
CdcCitusHasBeenLoaded()
{
	if (!IsCitusExtensionLoaded)
	{
		IsCitusExtensionLoaded = (get_extension_oid("citus", true) != InvalidOid);
	}

	return IsCitusExtensionLoaded;
}


/*
 * ExtractShardIdFromTableName tries to extract shard id from the given table name,
 * and returns the shard id if table name is formatted as shard name.
 * Else, the function returns INVALID_SHARD_ID.
 */
uint64
CdcExtractShardIdFromTableName(const char *tableName, bool missingOk)
{
	char *shardIdStringEnd = NULL;

	/* find the last underscore and increment for shardId string */
	char *shardIdString = strrchr(tableName, SHARD_NAME_SEPARATOR);
	if (shardIdString == NULL && !missingOk)
	{
		ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
							   tableName)));
	}
	else if (shardIdString == NULL && missingOk)
	{
		return INVALID_SHARD_ID;
	}

	shardIdString++;

	errno = 0;
	uint64 shardId = strtoull(shardIdString, &shardIdStringEnd, 0);

	if (errno != 0 || (*shardIdStringEnd != '\0'))
	{
		if (!missingOk)
		{
			ereport(ERROR, (errmsg("could not extract shardId from table name \"%s\"",
								   tableName)));
		}
		else
		{
			return INVALID_SHARD_ID;
		}
	}

	return shardId;
}


/*
 * CdcGetLocalGroupId returns the group identifier of the local node. The function assumes
 * that pg_dist_local_node_group has exactly one row and has at least one column.
 * Otherwise, the function errors out.
 */
static int32
CdcGetLocalGroupId(void)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	int32 groupId = 0;

	/*
	 * Already set the group id, no need to read the heap again.
	 */
	if (LocalGroupId != -1)
	{
		return LocalGroupId;
	}

	Oid localGroupTableOid = DistLocalGroupIdRelationId();
	if (localGroupTableOid == InvalidOid)
	{
		return 0;
	}

	Relation pgDistLocalGroupId = table_open(localGroupTableOid, AccessShareLock);

	SysScanDesc scanDescriptor = systable_beginscan(pgDistLocalGroupId,
													InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistLocalGroupId);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Datum groupIdDatum = heap_getattr(heapTuple,
										  Anum_pg_dist_local_groupid,
										  tupleDescriptor, &isNull);

		groupId = DatumGetInt32(groupIdDatum);

		/* set the local cache variable */
		LocalGroupId = groupId;
	}
	else
	{
		/*
		 * Upgrade is happening. When upgrading postgres, pg_dist_local_group is
		 * temporarily empty before citus_finish_pg_upgrade() finishes execution.
		 */
		groupId = GROUP_ID_UPGRADING;
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistLocalGroupId, AccessShareLock);

	return groupId;
}


/*
 * CdcLookupShardRelationFromCatalog returns the logical relation oid a shard belongs to.
 *
 * Errors out if the shardId does not exist and missingOk is false.
 * Returns InvalidOid if the shardId does not exist and missingOk is true.
 */
Oid
CdcLookupShardRelationFromCatalog(int64 shardId, bool missingOk)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Form_pg_dist_shard shardForm = NULL;
	Relation pgDistShard = table_open(DistShardRelationId(), AccessShareLock);
	Oid relationId = InvalidOid;

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistShard,
													DistShardShardidIndexId(), true,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple) && !missingOk)
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}

	if (!HeapTupleIsValid(heapTuple))
	{
		relationId = InvalidOid;
	}
	else
	{
		shardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
		relationId = shardForm->logicalrelid;
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistShard, NoLock);

	return relationId;
}


/*
 * CdcPgDistPartitionTupleViaCatalog is a helper function that searches
 * pg_dist_partition for the given relationId. The caller is responsible
 * for ensuring that the returned heap tuple is valid before accessing
 * its fields.
 */
static HeapTuple
CdcPgDistPartitionTupleViaCatalog(Oid relationId)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK, NULL, scanKeyCount, scanKey);

	HeapTuple partitionTuple = systable_getnext(scanDescriptor);

	if (HeapTupleIsValid(partitionTuple))
	{
		/* callers should have the tuple in their memory contexts */
		partitionTuple = heap_copytuple(partitionTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, AccessShareLock);

	return partitionTuple;
}


/*
 * CdcPartitionMethodViaCatalog gets a relationId and returns the partition
 * method column from pg_dist_partition via reading from catalog.
 */
char
CdcPartitionMethodViaCatalog(Oid relationId)
{
	HeapTuple partitionTuple = CdcPgDistPartitionTupleViaCatalog(relationId);
	if (!HeapTupleIsValid(partitionTuple))
	{
		return DISTRIBUTE_BY_INVALID;
	}

	Datum datumArray[Natts_pg_dist_partition];
	bool isNullArray[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	heap_deform_tuple(partitionTuple, tupleDescriptor, datumArray, isNullArray);

	if (isNullArray[Anum_pg_dist_partition_partmethod - 1])
	{
		/* partition method cannot be NULL, still let's make sure */
		heap_freetuple(partitionTuple);
		table_close(pgDistPartition, NoLock);
		return DISTRIBUTE_BY_INVALID;
	}

	Datum partitionMethodDatum = datumArray[Anum_pg_dist_partition_partmethod - 1];
	char partitionMethodChar = DatumGetChar(partitionMethodDatum);

	heap_freetuple(partitionTuple);
	table_close(pgDistPartition, NoLock);

	return partitionMethodChar;
}


/*
 * RemoveCitusDecodersFromPaths removes a path ending in citus_decoders
 * from the given input paths.
 */
char *
RemoveCitusDecodersFromPaths(char *paths)
{
	if (strlen(paths) == 0)
	{
		/* dynamic_library_path is empty */
		return paths;
	}

	StringInfo newPaths = makeStringInfo();

	char *remainingPaths = paths;

	for (;;)
	{
		int pathLength = 0;

		char *pathStart = first_path_var_separator(remainingPaths);
		if (pathStart == remainingPaths)
		{
			/*
			 * This will error out in find_in_dynamic_libpath, return
			 * original value here.
			 */
			return paths;
		}
		else if (pathStart == NULL)
		{
			/* final path */
			pathLength = strlen(remainingPaths);
		}
		else
		{
			/* more paths remaining */
			pathLength = pathStart - remainingPaths;
		}

		char *currentPath = palloc(pathLength + 1);
		strlcpy(currentPath, remainingPaths, pathLength + 1);
		canonicalize_path(currentPath);

		if (!pg_str_endswith(currentPath, "/citus_decoders"))
		{
			appendStringInfo(newPaths, "%s%s", newPaths->len > 0 ? ":" : "", currentPath);
		}

		if (remainingPaths[pathLength] == '\0')
		{
			/* end of string */
			break;
		}

		remainingPaths += pathLength + 1;
	}

	return newPaths->data;
}
