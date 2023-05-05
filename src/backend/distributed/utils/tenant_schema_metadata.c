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
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/pg_dist_tenant_schema.h"
#include "distributed/tenant_schema_metadata.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"


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
	ScanKeyInit(&scanKey[0], Anum_pg_dist_tenant_schema_schemaid, BTEqualStrategyNumber,
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
						 Anum_pg_dist_tenant_schema_colocationid,
						 RelationGetDescr(pgDistTenantSchema),
						 &isNull));
		Assert(!isNull);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistTenantSchema, AccessShareLock);

	return colocationId;
}


/*
 * ColocationIdGetTenantSchemaId returns the oid of the tenant schame that
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
	ScanKeyInit(&scanKey[0], Anum_pg_dist_tenant_schema_colocationid,
				BTEqualStrategyNumber, F_INT4EQ, UInt32GetDatum(colocationId));

	bool indexOk = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistTenantSchema,
													DistTenantSchemaUniqueColocationIdIndexId(),
													indexOk, NULL, 1, scanKey);

	HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
	Oid schemaId = InvalidOid;
	if (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		schemaId = heap_getattr(heapTuple, Anum_pg_dist_tenant_schema_schemaid,
								RelationGetDescr(pgDistTenantSchema), &isNull);
		Assert(!isNull);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistTenantSchema, AccessShareLock);

	return schemaId;
}


/*
 * InsertTenantSchemaLocally inserts an entry into pg_dist_tenant_schema
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

	Datum values[Natts_pg_dist_tenant_schema] = { 0 };
	bool isNulls[Natts_pg_dist_tenant_schema] = { 0 };

	values[Anum_pg_dist_tenant_schema_schemaid - 1] = ObjectIdGetDatum(schemaId);
	values[Anum_pg_dist_tenant_schema_colocationid - 1] = UInt32GetDatum(colocationId);

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
 * pg_dist_tenant_schema.
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
	ScanKeyInit(&scanKey[0], Anum_pg_dist_tenant_schema_schemaid, BTEqualStrategyNumber,
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
