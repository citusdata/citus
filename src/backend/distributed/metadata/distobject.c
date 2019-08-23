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

#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"


PG_FUNCTION_INFO_V1(master_unmark_object_distributed);


/*
 * master_unmark_object_distributed(classid oid, objid oid, objsubid int)
 *
 * removes the entry for an object address from pg_dist_object. Only removes the entry if
 * the object does not exist anymore.
 */
Datum
master_unmark_object_distributed(PG_FUNCTION_ARGS)
{
	Oid classid = PG_GETARG_OID(0);
	Oid objid = PG_GETARG_OID(1);
	int32 objsubid = PG_GETARG_INT32(2);

	ObjectAddress address = { 0 };
	ObjectAddressSubSet(address, classid, objid, objsubid);

	if (!IsObjectDistributed(&address))
	{
		/* if the object is not distributed there is no need to unmark it */
		PG_RETURN_VOID();
	}

	if (ObjectExsists(&address))
	{
		ereport(ERROR, (errmsg("object still exists"),
						errdetail("the %s \"%s\" still exists",
								  getObjectTypeDescription(&address),
								  getObjectIdentity(&address)),
						errhint("drop the object via a DROP command")));
	}

	UnmarkObjectDistributed(&address);

	PG_RETURN_VOID();
}


/*
 * ObjectExsists checks if an object given by its object address exists
 *
 * This is done by opening the catalog for the object and search the catalog for the
 * objects' oid. If we can find a tuple the object is existing. If no tuple is found, or
 * we don't have the information to find the tuple by its oid we assume the object does
 * not exist.
 */
bool
ObjectExsists(const ObjectAddress *address)
{
	if (is_objectclass_supported(address->classId))
	{
		HeapTuple objtup;
		Relation catalog = heap_open(address->classId, AccessShareLock);

		objtup = get_catalog_object_by_oid(catalog, address->objectId);
		heap_close(catalog, AccessShareLock);
		if (objtup != NULL)
		{
			return true;
		}

		return false;
	}

	return false;
}


/*
 * MarkObjectDistributed marks an object as a distributed object by citus. Marking is done
 * by adding appropriate entries to citus.pg_dist_object
 */
void
MarkObjectDistributed(const ObjectAddress *distAddress)
{
	Relation pgDistObject = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_object];
	bool newNulls[Natts_pg_dist_object];

	/* open system catalog and insert new tuple */
	pgDistObject = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_object */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, true, sizeof(newNulls));

	/* tuple (classId, objectId, NULL) */
	newValues[Anum_pg_dist_object_classid - 1] = ObjectIdGetDatum(distAddress->classId);
	newNulls[Anum_pg_dist_object_classid - 1] = false;
	newValues[Anum_pg_dist_object_objid - 1] = ObjectIdGetDatum(distAddress->objectId);
	newNulls[Anum_pg_dist_object_objid - 1] = false;
	newValues[Anum_pg_dist_object_objsubid - 1] = Int32GetDatum(distAddress->objectSubId);
	newNulls[Anum_pg_dist_object_objsubid - 1] = false;

	newTuple = heap_form_tuple(RelationGetDescr(pgDistObject), newValues, newNulls);

	CatalogTupleInsert(pgDistObject, newTuple);

	CommandCounterIncrement();
	heap_close(pgDistObject, NoLock);
}


/*
 * UnmarkObjectDistributed removes the entry from pg_dist_object that marks this object as
 * distributed. This will prevent updates to that object to be propagated to the worker.
 */
void
UnmarkObjectDistributed(const ObjectAddress *address)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[3] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;

	pgDistObjectRel = heap_open(DistObjectRelationId(), RowExclusiveLock);

	/* scan pg_dist_object for classid = $1 AND objid = $2 using an index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->objectId));
	ScanKeyInit(&key[2], Anum_pg_dist_object_objsubid, BTEqualStrategyNumber, F_INT4EQ,
				ObjectIdGetDatum(address->objectSubId));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel,
										  DistObjectClassIDObjectIDObjectSubIdIndexId(),
										  true, NULL, 3, key);

	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		CatalogTupleDelete(pgDistObjectRel, &pgDistObjectTup->t_self);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, RowExclusiveLock);
}


/*
 * IsObjectDistributed returns if the object addressed is already distributed in the
 * cluster. This performs a local indexed lookup in pg_dist_object.
 */
bool
IsObjectDistributed(const ObjectAddress *address)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[3] = { 0 };
	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;
	bool result = false;

	pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = $1 AND objid = $2 AND objsubid = $3 via index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->objectId));
	ScanKeyInit(&key[2], Anum_pg_dist_object_objsubid, BTEqualStrategyNumber, F_INT4EQ,
				ObjectIdGetDatum(address->objectSubId));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel,
										  DistObjectClassIDObjectIDObjectSubIdIndexId(),
										  true, NULL, 3, key);

	pgDistObjectTup = systable_getnext(pgDistObjectScan);
	if (HeapTupleIsValid(pgDistObjectTup))
	{
		result = true;
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return result;
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
		ObjectAddressSubSet(*objectAddress,
							pg_dist_object->classid,
							pg_dist_object->objid,
							pg_dist_object->objsubid);
		objectAddressList = lappend(objectAddressList, objectAddress);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return objectAddressList;
}
