/*-------------------------------------------------------------------------
 *
 * drop_timeseries_table.c
 *	  Routines related to the drop of timeseries tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "storage/lockdefs.h"
#include "utils/elog.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"

#include "distributed/metadata_cache.h"
#include "timeseries/timeseries_utils.h"

PG_FUNCTION_INFO_V1(drop_timeseries_table);

/*
 * drop_timeseries_table gets the table oid, then it drops
 * all the metadata related to it. Note that this function doesn't
 * drop any partitions or any data of the given table.
 *
 * TODO: Add unscheduling for automated jobs as well.
 */
Datum
drop_timeseries_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_VOID();
	}

	Oid relationId = PG_GETARG_OID(0);

	ScanKeyData relIdKey[1];
	Relation timeseriesRelation = table_open(CitusTimeseriesTablesRelationId(),
											 AccessShareLock);

	ScanKeyInit(&relIdKey[0],
				Anum_citus_timeseries_table_relation_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc timeseriesRelScan = systable_beginscan(timeseriesRelation, InvalidOid,
													   false, NULL, 1, relIdKey);
	HeapTuple timeseriesTuple = systable_getnext(timeseriesRelScan);

	if (HeapTupleIsValid(timeseriesTuple))
	{
		CatalogTupleDelete(timeseriesRelation, &timeseriesTuple->t_self);
		CommandCounterIncrement();
	}

	systable_endscan(timeseriesRelScan);
	table_close(timeseriesRelation, NoLock);

	PG_RETURN_VOID();
}
