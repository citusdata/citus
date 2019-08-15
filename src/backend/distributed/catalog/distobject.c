/*-------------------------------------------------------------------------
 *
 * distobject.c
 *	  Functions to interact with distributed objects by their ObjectAddress
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

#include "distributed/dist_catalog/distobject.h"
#include "distributed/dist_catalog/pg_dist_object.h"
#include "distributed/metadata_cache.h"


PG_FUNCTION_INFO_V1(citus_prepare_pg_upgrade_pg_dist_object);
PG_FUNCTION_INFO_V1(citus_finish_pg_upgrade_pg_dist_object);


/*
 * getObjectAddressByIdentifier provides the ObjectAddress for an object of type classid
 * with the fully qualified name of identifier.
 *
 * The goal of this function is to be functionally reverse of getObjectIdentity
 */
ObjectAddress *
getObjectAddressByIdentifier(Oid classId, const char *identifier)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));

	switch (classId)
	{
		case NamespaceRelationId:
		{
			List *names = NIL;
			const char *namespaceName = NULL;
			Oid namespaceOid = InvalidOid;

			names = stringToQualifiedNameList(identifier);
			Assert(list_length(names) == 1);

			namespaceName = strVal(linitial(names));
			namespaceOid = get_namespace_oid(namespaceName, false);

			ObjectAddressSet(*address, NamespaceRelationId, namespaceOid);

			return address;
		};

		default:
		{
			ereport(ERROR, (errmsg("unrecognized object class: %u", classId)));
		}
	}
}


/*
 * markObjectDistributed marks an object as a distributed object by citus. Marking is done
 * by adding appropriate entries to citus.pg_dist_object
 */
void
markObjectDistributed(const ObjectAddress *distAddress)
{
	Relation pgDistObject = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_object];
	bool newNulls[Natts_pg_dist_object];

	/* open system catalog and insert new tuple */
	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_object */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	/* tuple (classId, objectId, NULL) */
	newValues[Anum_pg_dist_object_classid - 1] = ObjectIdGetDatum(distAddress->classId);
	newValues[Anum_pg_dist_object_objid - 1] = ObjectIdGetDatum(distAddress->objectId);
	newNulls[Anum_pg_dist_object_identifier - 1] = true;

	newTuple = heap_form_tuple(RelationGetDescr(pgDistObject), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	CatalogTupleInsert(pgDistObject, newTuple);

	CommandCounterIncrement();
	heap_close(pgDistObject, NoLock);
}


/*
 * unmarkObjectDistributed removes the entry from pg_dist_object that marks this object as
 * distributed. This will prevent updates to that object to be propagated to the worker.
 */
void
unmarkObjectDistributed(const ObjectAddress *address)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;

	pgDistObjectRel = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* scan pg_dist_object for classid = $1 AND objid = $2 using an index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->objectId));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel,
										  DistObjectClassIDObjectIDIndexId(), true, NULL,
										  2, key);

	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		CatalogTupleDelete(pgDistObjectRel, &pgDistObjectTup->t_self);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, RowExclusiveLock);
}


/*
 * isObjectDistributed returns if the object addressed is already distributed in the
 * cluster. This performs a local indexed lookup in pg_dist_object.
 */
bool
isObjectDistributed(const ObjectAddress *address)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;
	bool result = false;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = $1 AND objid = $2 using an index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->objectId));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel,
										  DistObjectClassIDObjectIDIndexId(), true, NULL,
										  2, key);

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
 * citus_prepare_pg_upgrade_pg_dist_object prepares pg_dist_object before a pg_upgrade. It
 * stores an upgrade stable identifier of the objects in the identifier column which can
 * later be restored to the object id of the object.
 */
Datum
citus_prepare_pg_upgrade_pg_dist_object(PG_FUNCTION_ARGS)
{
	Relation pgDistObject = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	HeapTuple newHeapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];
	Datum datumArray[Natts_pg_dist_object];

	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistObject);

	scanDescriptor = systable_beginscan(pgDistObject, InvalidOid, false, NULL,
										0, NULL);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Oid classId = InvalidOid;
		Oid objectId = InvalidOid;
		const char *identifier = NULL;
		ObjectAddress address = { 0 };

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isnull);

		memset(replace, false, sizeof(replace));
		memset(isnull, false, sizeof(isnull));
		memset(values, 0, sizeof(values));

		/* get identifier based on ObjectAddress */
		classId = DatumGetObjectId(datumArray[Anum_pg_dist_object_classid - 1]);
		objectId = DatumGetObjectId(datumArray[Anum_pg_dist_object_objid - 1]);
		ObjectAddressSet(address, classId, objectId);
		identifier = getObjectIdentity(&address);

		/* update tuple SET identifier = $1 */
		values[Anum_pg_dist_object_identifier - 1] = CStringGetTextDatum(identifier);
		replace[Anum_pg_dist_object_identifier - 1] = true;

		newHeapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
										 replace);
		CatalogTupleUpdate(pgDistObject, &newHeapTuple->t_self, newHeapTuple);
		heap_freetuple(newHeapTuple);
	}

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistObject, NoLock);

	PG_RETURN_VOID();
}


/*
 * citus_finish_pg_upgrade_pg_dist_object runs after a pg_upgrade to restore the oid's of
 * the objects citus had distributed in the past. It also clears the identifier column
 * again.
 */
Datum
citus_finish_pg_upgrade_pg_dist_object(PG_FUNCTION_ARGS)
{
	Relation pgDistObject = NULL;
	SysScanDesc scanDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	HeapTuple newHeapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

	Datum values[Natts_pg_dist_object];
	bool isnull[Natts_pg_dist_object];
	bool replace[Natts_pg_dist_object];
	Datum datumArray[Natts_pg_dist_object];

	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgDistObject);

	scanDescriptor = systable_beginscan(pgDistObject, InvalidOid, false, NULL,
										0, NULL);

	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Oid classId = InvalidOid;
		const char *identifier;
		ObjectAddress *address = NULL;

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isnull);

		memset(replace, false, sizeof(replace));
		memset(isnull, false, sizeof(isnull));
		memset(values, 0, sizeof(values));

		classId = DatumGetObjectId(datumArray[Anum_pg_dist_object_classid - 1]);
		identifier = TextDatumGetCString(datumArray[Anum_pg_dist_object_identifier - 1]);
		address = getObjectAddressByIdentifier(classId, identifier);

		/* update tuple SET objid = $1 AND identifier = NULL*/
		values[Anum_pg_dist_object_objid - 1] = ObjectIdGetDatum(address->objectId);
		replace[Anum_pg_dist_object_objid - 1] = true;
		isnull[Anum_pg_dist_object_identifier - 1] = true;
		replace[Anum_pg_dist_object_identifier - 1] = true;

		newHeapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
										 replace);
		CatalogTupleUpdate(pgDistObject, &newHeapTuple->t_self, newHeapTuple);
		heap_freetuple(newHeapTuple);
	}

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	heap_close(pgDistObject, NoLock);

	PG_RETURN_VOID();
}


/*
 * GetDistributedObjectAddressList returns a list of ObjectAddresses that contains all
 * distributed objects as marked in pg_dist_object
 */
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
