/*-------------------------------------------------------------------------
 *
 * shardgroup.c
 *
 * This file contains functions to perform useful operations on shardgroups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "utils/fmgroids.h"
#include "access/skey.h"
#include "utils/relcache.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"

#include "distributed/listutils.h"
#include "distributed/shardgroup.h"
#include "distributed/pg_dist_shardgroup.h"
#include "distributed/metadata_cache.h"

List *
ShardgroupForColocationId(uint32 colocationId)
{
	ScanKeyData scanKey[1] = { 0 };
	Form_pg_dist_shardgroup shardgroupForm = NULL;
	Relation pgDistShardgroup = table_open(DistShardgroupRelationId(), AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistShardgroup);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shardgroup_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, UInt32GetDatum(colocationId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistShardgroup, DistShardgroupColocaionidIndexId(), true,
						   NULL, lengthof(scanKey), scanKey);

	HeapTuple heapTuple = NULL;
	List *shardgroups = NIL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum datumArray[Natts_pg_dist_shardgroup] = { 0 };
		bool isNullArray[Natts_pg_dist_shardgroup] = { 0 };
		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		shardgroupForm = (Form_pg_dist_shardgroup) GETSTRUCT(heapTuple);

		Shardgroup *shardgroup = palloc0(sizeof(Shardgroup));
		shardgroup->shardgroupId = shardgroupForm->shardgroupid;
		shardgroup->colocationId = shardgroupForm->colocationid;

		/* load shard min/max values */
		Datum minValueTextDatum = datumArray[Anum_pg_dist_shardgroup_shardminvalue - 1];
		Datum maxValueTextDatum = datumArray[Anum_pg_dist_shardgroup_shardmaxvalue - 1];
		shardgroup->minShardValue = TextDatumGetCString(minValueTextDatum);
		shardgroup->maxShardValue = TextDatumGetCString(maxValueTextDatum);


		shardgroups = lappend(shardgroups, shardgroup);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistShardgroup, NoLock);

	return shardgroups;
}
