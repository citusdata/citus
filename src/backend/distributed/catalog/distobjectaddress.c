/*-------------------------------------------------------------------------
 *
 * distobjectaddress.c
 *	  Functions to work with object addresses of distributed objects.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "access/skey.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_namespace.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "utils/fmgroids.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/builtins.h"

#include "distributed/dist_catalog/distobjectaddress.h"
#include "distributed/dist_catalog/pg_dist_object.h"
#include "distributed/metadata_cache.h"


PG_FUNCTION_INFO_V1(citus_update_dist_object_oids);


/*
 * getDistObjectAddressFromPg maps a postgres object address to a citus distributed object
 * address.
 */
DistObjectAddress *
getDistObjectAddressFromPg(const ObjectAddress *address)
{
	DistObjectAddress *distAddress = palloc0(sizeof(DistObjectAddress));

	distAddress->classId = address->classId;
	distAddress->objectId = address->objectId;
	distAddress->identifier = getObjectIdentity(address);

	return distAddress;
}


/*
 * getObjectAddresFromCitus maps a citus distributed object address to a postgres object
 * address
 */
ObjectAddress *
getObjectAddresFromCitus(const DistObjectAddress *distAddress)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	switch (distAddress->classId)
	{
		case NamespaceRelationId:
		{
			List *names = NIL;
			const char *namespaceName = NULL;
			Oid namespaceOid = InvalidOid;

			names = stringToQualifiedNameList(distAddress->identifier);
			Assert(list_length(names) == 1);

			namespaceName = strVal(linitial(names));
			namespaceOid = get_namespace_oid(namespaceName, false);

			ObjectAddressSet(*address, NamespaceRelationId, namespaceOid);

			return address;
		};

		default:
		{
			ereport(ERROR, (errmsg("unrecognized object class: %u",
								   distAddress->classId)));
		}
	}
}


/*
 * makeDistObjectAddress creates a newly allocated DistObjectAddress that points to the
 * classId and identifier passed. No checks are performed to verify the object exists.
 */
DistObjectAddress *
makeDistObjectAddress(Oid classid, Oid objectid, const char *identifier)
{
	DistObjectAddress *distAddress = palloc0(sizeof(DistObjectAddress));
	distAddress->classId = classid;
	distAddress->objectId = objectid;
	distAddress->identifier = pstrdup(identifier);
	return distAddress;
}


/*
 * recordObjectDistributedByAddress marks an object as a distributed object by is postgres
 * address.
 */
void
recordObjectDistributedByAddress(const ObjectAddress *address)
{
	recordObjectDistributed(getDistObjectAddressFromPg(address));
}


/*
 * recordObjectDistributed mark an object as a distributed object in citus.
 */
void
recordObjectDistributed(const DistObjectAddress *distAddress)
{
	Relation pgDistObject = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_object];
	bool newNulls[Natts_pg_dist_object];

	/* open system catalog and insert new tuple */
	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_object_classid - 1] = ObjectIdGetDatum(distAddress->classId);
	newValues[Anum_pg_dist_object_objid - 1] = ObjectIdGetDatum(distAddress->objectId);
	newValues[Anum_pg_dist_object_identifier - 1] = CStringGetTextDatum(
		distAddress->identifier);

	newTuple = heap_form_tuple(RelationGetDescr(pgDistObject), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	CatalogTupleInsert(pgDistObject, newTuple);

	CommandCounterIncrement();
	heap_close(pgDistObject, NoLock);
}


void
dropObjectDistributedByAddress(const ObjectAddress *address)
{
	dropObjectDistributed(getDistObjectAddressFromPg(address));
}


void
dropObjectDistributed(const DistObjectAddress *distAddress)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;

	pgDistObjectRel = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* scan pg_dist_object for classid = $1 AND identifier = $2 */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_identifier, BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(distAddress->identifier));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel, InvalidOid, false, NULL, 2,
										  key);

	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		CatalogTupleDelete(pgDistObjectRel, &pgDistObjectTup->t_self);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, RowExclusiveLock);
}


/*
 * isObjectDistributedByAddress returns if the postgres object addressed by address is
 * know to be distributed by citus.
 */
bool
isObjectDistributedByAddress(const ObjectAddress *address)
{
	return isObjectDistributed(getDistObjectAddressFromPg(address));
}


/*
 * isObjectDistributed returns if the object identified by the distAddress is already
 * distributed in the cluster. This performs a local lookup in pg_dist_object.
 */
bool
isObjectDistributed(const DistObjectAddress *distAddress)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;
	bool result = false;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = $1 AND identifier = $2 */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(distAddress->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_identifier, BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(distAddress->identifier));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel, InvalidOid, false, NULL, 2,
										  key);

	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		/* tuple found, we are done */
		result = true;
		break;
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return result;
}


/*
 * citus_update_dist_object_oids updates the objid column of pg_dist_objects table with
 * the current object id's based on the identifier of the object. This is done by a full
 * table scan over all identifiers, then resolve these identifiers to their object id.
 */
Datum
citus_update_dist_object_oids(PG_FUNCTION_ARGS)
{
	Relation pgDistObject = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[0];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	HeapTuple newHeapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];
	Datum datumArray[Natts_pg_dist_object];

	/* we first search for colocation group by its colocation id */
	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistObject);

	scanDescriptor = systable_beginscan(pgDistObject, InvalidOid, false, NULL,
										scanKeyCount, scanKey);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		DistObjectAddress distAddress = { 0 };
		ObjectAddress *address = NULL;

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isnull);

		/* after we find colocation group, we update it with new values */
		memset(replace, false, sizeof(replace));
		memset(isnull, false, sizeof(isnull));
		memset(values, 0, sizeof(values));

		distAddress.classId = DatumGetObjectId(
			datumArray[Anum_pg_dist_object_classid - 1]);
		distAddress.objectId = DatumGetObjectId(
			datumArray[Anum_pg_dist_object_objid - 1]);
		distAddress.identifier = TextDatumGetCString(
			datumArray[Anum_pg_dist_object_identifier - 1]);

		address = getObjectAddresFromCitus(&distAddress);

		values[Anum_pg_dist_object_objid - 1] = ObjectIdGetDatum(address->objectId);
		replace[Anum_pg_dist_object_objid - 1] = true;

		newHeapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
										 replace);
		CatalogTupleUpdate(pgDistObject, &newHeapTuple->t_self, newHeapTuple);
		heap_freetuple(newHeapTuple);
	}

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistObject, NoLock);

	PG_RETURN_NULL();
}


List *
GetDistributedObjectAddressList(void)
{
	Relation pgDistObjectRel = NULL;
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;
	List *objectAddressList = NIL;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);
	pgDistObjectScan = systable_beginscan(pgDistObjectRel, InvalidOid, false, NULL, 0,
										  NULL);
	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		Form_pg_dist_object pg_dist_object =
			(Form_pg_dist_object) GETSTRUCT(pgDistObjectTup);
		ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*objectAddress, pg_dist_object->classid, pg_dist_object->objid);
		objectAddressList = lappend(objectAddressList, objectAddress);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return objectAddressList;
}
