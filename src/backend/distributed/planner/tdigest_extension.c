/*-------------------------------------------------------------------------
 *
 * tdigest_extension.c
 *    Helper functions to get access to tdigest specific data.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_type.h"
#include "utils/fmgroids.h"
#include "catalog/pg_extension.h"
#include "distributed/metadata_cache.h"
#include "distributed/tdigest_extension.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"


/*
 * TDigestExtensionSchema finds the schema the tdigest extension is installed in. The
 * function will return InvalidOid if the extension is not installed.
 */
Oid
TDigestExtensionSchema()
{
	ScanKeyData entry[1];
	Form_pg_extension extensionForm = NULL;
	Oid tdigestExtensionSchema = InvalidOid;

	Relation relation = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("tdigest"));

	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, true,
											  NULL, 1, entry);

	HeapTuple extensionTuple = systable_getnext(scandesc);

	/*
	 * We assume that there can be at most one matching tuple, if no tuple found the
	 * extension is not installed. The value of InvalidOid will not be changed.
	 */
	if (HeapTupleIsValid(extensionTuple))
	{
		extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);
		tdigestExtensionSchema = extensionForm->extnamespace;
		Assert(OidIsValid(tdigestExtensionSchema));
	}

	systable_endscan(scandesc);

	heap_close(relation, AccessShareLock);

	return tdigestExtensionSchema;
}


/*
 * TDigestExtensionTypeOid preforms a lookup for the Oid of the type representing the
 * tdigest as installed by the tdigest extension returns InvalidOid if the type cannot be
 * found.
 */
Oid
TDigestExtensionTypeOid()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}
	char *namespaceName = get_namespace_name(tdigestSchemaOid);
	return LookupTypeOid(namespaceName, "tdigest");
}


/*
 * TDigestExtensionAggTDigest1 performs a lookup for the Oid of the tdigest aggregate;
 *   tdigest(tdigest)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigest1()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *namespaceName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(list_make2(makeString(namespaceName), makeString("tdigest")),
						  1, (Oid[]) { TDigestExtensionTypeOid() }, true);
}


/*
 * TDigestExtensionAggTDigest2 performs a lookup for the Oid of the tdigest aggregate;
 *   tdigest(value double precision, compression int)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigest2()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(list_make2(makeString(schemaName), makeString("tdigest")),
						  2, (Oid[]) { FLOAT8OID, INT4OID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentile2 performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile(tdigest, double precision)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentile2()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}
	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile")),
		2, (Oid[]) { TDigestExtensionTypeOid(), FLOAT8OID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentile2a performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile(tdigest, double precision[])
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentile2a(void)
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile")),
		2, (Oid[]) { TDigestExtensionTypeOid(), FLOAT8ARRAYOID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentile3 performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile(double precision, int, double precision)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentile3()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile")),
		3, (Oid[]) { FLOAT8OID, INT4OID, FLOAT8OID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentile3a performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile(double precision, int, double precision[])
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentile3a(void)
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile")),
		3, (Oid[]) { FLOAT8OID, INT4OID, FLOAT8ARRAYOID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentileOf2 performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile_of(tdigest, double precision)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentileOf2()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile_of")),
		2, (Oid[]) { TDigestExtensionTypeOid(), FLOAT8OID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentileOf2a performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile_of(tdigest, double precision[])
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentileOf2a(void)
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile_of")),
		2, (Oid[]) { TDigestExtensionTypeOid(), FLOAT8ARRAYOID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentileOf3 performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile_of(double precision, int, double precision)
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentileOf3()
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile_of")),
		3, (Oid[]) { FLOAT8OID, INT4OID, FLOAT8OID }, true);
}


/*
 * TDigestExtensionAggTDigestPercentileOf3a performs a lookup for the Oid of the tdigest
 * aggregate;
 *   tdigest_percentile_of(double precision, int, double precision[])
 *
 * If the aggregate is not found InvalidOid is returned.
 */
Oid
TDigestExtensionAggTDigestPercentileOf3a(void)
{
	Oid tdigestSchemaOid = TDigestExtensionSchema();
	if (!OidIsValid(tdigestSchemaOid))
	{
		return InvalidOid;
	}

	char *schemaName = get_namespace_name(tdigestSchemaOid);
	return LookupFuncName(
		list_make2(makeString(schemaName), makeString("tdigest_percentile_of")),
		3, (Oid[]) { FLOAT8OID, INT4OID, FLOAT8ARRAYOID }, true);
}
