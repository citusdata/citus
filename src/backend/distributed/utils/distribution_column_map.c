/*-------------------------------------------------------------------------
 *
 * distribution_column_map.c
 *	  Implementation of a relation OID to distribution column map.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "nodes/primnodes.h"

#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/utils/distribution_column_map.h"


/*
 * RelationIdDistributionColumnMapEntry is used to map relation IDs to
 * distribution column Vars.
 */
typedef struct RelationIdDistributionColumnMapEntry
{
	/* OID of the relation */
	Oid relationId;

	/* a Var describing the distribution column */
	Var *distributionColumn;
} RelationIdDistributionColumnMapEntry;


/*
 * CreateDistributionColumnMap creates an empty (OID -> distribution column Var) map.
 */
DistributionColumnMap *
CreateDistributionColumnMap(void)
{
	HASHCTL info = { 0 };
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(RelationIdDistributionColumnMapEntry);
	info.hash = oid_hash;
	info.hcxt = CurrentMemoryContext;

	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB *distributionColumnMap = hash_create("Distribution Column Map", 32,
											  &info, hashFlags);

	return distributionColumnMap;
}


/*
 * AddDistributionColumnForRelation adds the given OID and its distribution column
 * to the hash, as well as any child partitions.
 */
void
AddDistributionColumnForRelation(DistributionColumnMap *distributionColumnMap,
								 Oid relationId,
								 char *distributionColumnName)
{
	bool entryFound = false;
	RelationIdDistributionColumnMapEntry *entry =
		hash_search(distributionColumnMap, &relationId, HASH_ENTER, &entryFound);

	Assert(!entryFound);

	entry->distributionColumn =
		BuildDistributionKeyFromColumnName(relationId, distributionColumnName, NoLock);

	if (PartitionedTable(relationId))
	{
		/*
		 * Recursively add partitions as well.
		 */
		List *partitionList = PartitionList(relationId);
		Oid partitionRelationId = InvalidOid;

		foreach_declared_oid(partitionRelationId, partitionList)
		{
			AddDistributionColumnForRelation(distributionColumnMap, partitionRelationId,
											 distributionColumnName);
		}
	}
}


/*
 * GetDistributionColumnFromMap returns the distribution column for a given
 * relation ID from the distribution column map.
 */
Var *
GetDistributionColumnFromMap(DistributionColumnMap *distributionColumnMap,
							 Oid relationId)
{
	bool entryFound = false;

	RelationIdDistributionColumnMapEntry *entry =
		hash_search(distributionColumnMap, &relationId, HASH_FIND, &entryFound);

	if (entryFound)
	{
		return entry->distributionColumn;
	}
	else
	{
		return NULL;
	}
}


/*
 * GetDistributionColumnWithOverrides returns the distribution column for a given
 * relation from the distribution column overrides map, or the metadata if no
 * override is specified.
 */
Var *
GetDistributionColumnWithOverrides(Oid relationId,
								   DistributionColumnMap *distributionColumnOverrides)
{
	Var *distributionColumn = NULL;

	if (distributionColumnOverrides != NULL)
	{
		distributionColumn = GetDistributionColumnFromMap(distributionColumnOverrides,
														  relationId);
		if (distributionColumn != NULL)
		{
			return distributionColumn;
		}
	}

	/* no override defined, use distribution column from metadata */
	return DistPartitionKey(relationId);
}
