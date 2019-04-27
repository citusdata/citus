/*-------------------------------------------------------------------------
 *
 * sharding.h
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARDING_H
#define SHARDING_H


#include "fmgr.h"


/*
 * DatumRange represents a range of values.
 */
typedef struct DatumRange
{
	Datum minValue;
	Datum maxValue;
} DatumRange;

/*
 * PartitionScheme represents a full partitioning scheme for a
 * distributed table.
 */
typedef struct PartitioningScheme
{
	/* number of partitions */
	int partitionCount;

	/* ordered range of values for each partition */
	DatumRange *ranges;

	/* min/max value datum's typeId */
	Oid valueTypeId;

	/* min/max value datum's typeMod */
	int32 valueTypeMod;

	/* min/max value datum's typelen */
	int valueTypeLen;

	/* min/max value datum's byval */
	bool valueByVal;
} PartitioningScheme;

/*
 * DistributionScheme is a partitioning scheme that assigns
 * partitions to nodes.
 */
typedef struct DistributionScheme
{
	PartitioningScheme partitioning;

	/*
	 * groupIds is an array group ID arrays, indicating where the partition
	 * at a given index is placed.
	 */
	int **groupIds;

	/* co-location ID for this sharding scheme (-1 if not co-located) */
	int colocationId;
} DistributionScheme;


#endif /* SHARDING_H */
