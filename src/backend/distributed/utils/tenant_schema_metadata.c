/*-------------------------------------------------------------------------
 *
 * tenant_schema_metadata.c
 *
 * This file contains functions to query and modify tenant schema metadata,
 * which is used to track the schemas used for schema-based sharding in
 * Citus.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/table.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"

#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/pg_dist_schema.h"
#include "distributed/tenant_schema_metadata.h"


/*
 * IsTenantSchema returns true if there is a tenant schema with given schemaId.
 */
bool
IsTenantSchema(Oid schemaId)
{
	/*
	 * We don't allow creating tenant schemas when there is a version
	 * mismatch. Even more, SchemaIdGetTenantColocationId() would throw an
	 * error if the underlying pg_dist_schema metadata table has not
	 * been created yet, which is the case in older versions. For this reason,
	 * it's safe to assume that it cannot be a tenant schema when there is a
	 * version mismatch.
	 *
	 * But it's a bit tricky that we do the same when version checks are
	 * disabled because then CheckCitusVersion() returns true even if there
	 * is a version mismatch. And in that case, the tests that are trying to
	 * create tables (in multi_extension.sql) in older versions would
	 * fail when deciding whether we should create a tenant table or not.
	 *
	 * The downside of doing so is that, for example, we will skip deleting
	 * the tenant schema entry from pg_dist_schema when dropping a
	 * tenant schema while the version checks are disabled even if there was
	 * no version mismatch. But we're okay with that because we don't expect
	 * users to disable version checks anyway.
	 */
	if (!EnableVersionChecks || !CheckCitusVersion(DEBUG4))
	{
		return false;
	}

	return SchemaIdGetTenantColocationId(schemaId) != INVALID_COLOCATION_ID;
}


/*
 * IsTenantSchemaColocationGroup returns true if there is a tenant schema
 * that is associated with given colocation id.
 */
bool
IsTenantSchemaColocationGroup(uint32 colocationId)
{
	return OidIsValid(ColocationIdGetTenantSchemaId(colocationId));
}


/*
 * SchemaIdGetTenantColocationId returns the colocation id associated with
 * the tenant schema with given id.
 *
 * Returns INVALID_COLOCATION_ID if there is no tenant schema with given id.
 */
uint32
SchemaIdGetTenantColocationId(Oid schemaId)
{
	uint32 colocationId = INVALID_COLOCATION_ID;

	if (!OidIsValid(schemaId))
	{
		ereport(ERROR, (errmsg("schema id is invalid")));
	}

	Relation pgDistTenantSchema = table_open(DistTenantSchemaRelationId(),
											 AccessShareLock);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_pg_dist_schema_schemaid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(schemaId));

	bool indexOk = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistTenantSchema,
													DistTenantSchemaPrimaryKeyIndexId(),
													indexOk, NULL, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		colocationId = DatumGetUInt32(
			heap_getattr(heapTuple,
						 Anum_pg_dist_schema_colocationid,
						 RelationGetDescr(pgDistTenantSchema),
						 &isNull));
		Assert(!isNull);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistTenantSchema, AccessShareLock);

	return colocationId;
}


/*
 * ColocationIdGetTenantSchemaId returns the oid of the tenant schema that
 * is associated with given colocation id.
 *
 * Returns InvalidOid if there is no such tenant schema.
 */
Oid
ColocationIdGetTenantSchemaId(uint32 colocationId)
{
	if (colocationId == INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errmsg("colocation id is invalid")));
	}

	Relation pgDistTenantSchema = table_open(DistTenantSchemaRelationId(),
											 AccessShareLock);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_pg_dist_schema_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(colocationId));

	bool indexOk = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistTenantSchema,
													DistTenantSchemaUniqueColocationIdIndexId(),
													indexOk, NULL, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	Oid schemaId = InvalidOid;
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		schemaId = heap_getattr(heapTuple, Anum_pg_dist_schema_schemaid,
								RelationGetDescr(pgDistTenantSchema), &isNull);
		Assert(!isNull);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistTenantSchema, AccessShareLock);

	return schemaId;
}


/*
 * InsertTenantSchemaLocally inserts an entry into pg_dist_schema
 * with given schemaId and colocationId.
 *
 * Throws a constraint violation error if there is already an entry with
 * given schemaId, or if given colocation id is already associated with
 * another tenant schema.
 */
void
InsertTenantSchemaLocally(Oid schemaId, uint32 colocationId)
{
	if (!OidIsValid(schemaId))
	{
		ereport(ERROR, (errmsg("schema id is invalid")));
	}

	if (colocationId == INVALID_COLOCATION_ID)
	{
		ereport(ERROR, (errmsg("colocation id is invalid")));
	}

	Datum values[Natts_pg_dist_schema] = { 0 };
	bool isNulls[Natts_pg_dist_schema] = { 0 };

	values[Anum_pg_dist_schema_schemaid - 1] = ObjectIdGetDatum(schemaId);
	values[Anum_pg_dist_schema_colocationid - 1] = UInt32GetDatum(colocationId);

	Relation pgDistTenantSchema = table_open(DistTenantSchemaRelationId(),
											 RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistTenantSchema);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistTenantSchema, heapTuple);
	CommandCounterIncrement();

	table_close(pgDistTenantSchema, NoLock);
}


/*
 * DeleteTenantSchemaLocally deletes the entry for given schemaId from
 * pg_dist_schema.
 *
 * Throws an error if there is no such tenant schema.
 */
void
DeleteTenantSchemaLocally(Oid schemaId)
{
	if (!OidIsValid(schemaId))
	{
		ereport(ERROR, (errmsg("schema id is invalid")));
	}

	Relation pgDistTenantSchema = table_open(DistTenantSchemaRelationId(),
											 RowExclusiveLock);
	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0], Anum_pg_dist_schema_schemaid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(schemaId));

	bool indexOk = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistTenantSchema,
													DistTenantSchemaPrimaryKeyIndexId(),
													indexOk, NULL, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find tuple for tenant schema %u", schemaId)));
	}

	CatalogTupleDelete(pgDistTenantSchema, &heapTuple->t_self);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	table_close(pgDistTenantSchema, NoLock);
}
