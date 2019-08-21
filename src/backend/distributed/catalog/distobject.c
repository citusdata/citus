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
	newValues[Anum_pg_dist_object_objsubid - 1] = Int32GetDatum(distAddress->objectSubId);

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
