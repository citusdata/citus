/*-------------------------------------------------------------------------
 *
 * partitioning.c
 *	  Functions for dealing with partitioned tables.
 *
 * Copyright (c) Microsoft Corporation. All rights reserved.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(time_partition_range);


/*
 * time_partition_range returns the lower and upper bound of partition
 * key values for the partition of a time-partitioned table.
 */
Datum
time_partition_range(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	CheckCitusVersion(ERROR);

	/* create tuple descriptor for return value */
	TupleDesc metadataDescriptor = NULL;
	TypeFuncClass resultTypeClass = get_call_result_type(fcinfo, NULL,
														 &metadataDescriptor);
	if (resultTypeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("return type must be a row type")));
	}

	/* get the pg_class record */
	HeapTuple tuple = SearchSysCache1(RELOID, relationId);
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errmsg("relation with OID %u does not exist", relationId)));
	}

	/* get the pg_class record */
	bool isNull = false;
	Datum partitionBoundDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpartbound,
												&isNull);
	if (isNull)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is not a partition",
							   get_rel_name(relationId))));
	}

	PartitionBoundSpec *partitionBoundSpec =
		(PartitionBoundSpec *) stringToNode(TextDatumGetCString(partitionBoundDatum));

	if (!IsA(partitionBoundSpec, PartitionBoundSpec))
	{
		ereport(ERROR, (errmsg("expected PartitionBoundSpec")));
	}

	if (partitionBoundSpec->strategy != PARTITION_STRATEGY_RANGE)
	{
		ereport(ERROR, (errmsg("relation \"%s\" is not a range partition",
							   get_rel_name(relationId)),
						errdetail("time_partition_range can only be used for "
								  "partitions of range-partitioned tables with a single "
								  "partition column")));
	}

	Datum values[2];
	bool isNulls[2];

	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	if (partitionBoundSpec->is_default)
	{
		/* return NULL for default partition */
		isNulls[0] = true;
		isNulls[1] = true;
	}
	else
	{
		if (list_length(partitionBoundSpec->lowerdatums) != 1 ||
			list_length(partitionBoundSpec->upperdatums) != 1)
		{
			ereport(ERROR, (errmsg("relation \"%s\" is a partition with multiple "
								   "partition columns",
								   get_rel_name(relationId)),
							errdetail("time_partition_range can only be used for "
									  "partitions of range-partitioned tables with a "
									  "single partition column")));
		}

		PartitionRangeDatum *lowerBoundDatum =
			castNode(PartitionRangeDatum, linitial(partitionBoundSpec->lowerdatums));
		PartitionRangeDatum *upperBoundDatum =
			castNode(PartitionRangeDatum, linitial(partitionBoundSpec->upperdatums));

		Const *lowerConst = castNode(Const, lowerBoundDatum->value);
		Const *upperConst = castNode(Const, upperBoundDatum->value);

		char *lowerConstStr = DatumToString(lowerConst->constvalue,
											lowerConst->consttype);

		char *upperConstStr = DatumToString(upperConst->constvalue,
											upperConst->consttype);

		values[0] = CStringGetTextDatum(lowerConstStr);
		values[1] = CStringGetTextDatum(upperConstStr);
	}

	HeapTuple metadataTuple = heap_form_tuple(metadataDescriptor, values, isNulls);
	Datum metadataDatum = HeapTupleGetDatum(metadataTuple);

	ReleaseSysCache(tuple);

	PG_RETURN_DATUM(metadataDatum);
}
