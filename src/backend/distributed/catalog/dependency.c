/*-------------------------------------------------------------------------
 *
 * dependency.c
 *	  Functions to reason about distributed objects and their dependencies
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_depend.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "distributed/dist_catalog/dependency.h"
#include "distributed/dist_catalog/distobjectaddress.h"


static bool ShouldFollowDependency(const ObjectAddress *toFollow);
static bool IsObjectAddressInList(const ObjectAddress *findAddress, List *addressList);
static bool IsObjectAddressOwnedByExtension(const ObjectAddress *target);


/*
 * GetDependenciesForObject returns a list of ObjectAddesses to be created in order
 * before the target object could safely be created on a worker. Some of the object might
 * already be created on a worker. It should be created in an idempotent way.
 */
void
GetDependenciesForObject(const ObjectAddress *target, List **dependencyList)
{
	Relation depRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;

	depRel = heap_open(DependRelationId, AccessShareLock);

	/* scan pg_depend for classid = $1 AND objid = $2 using pg_depend_depender_index */
	ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->classId));
	ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->objectId));
	depScan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		ObjectAddress dependency = { 0 };
		ObjectAddress *dependencyPtr = NULL;
		ObjectAddressSet(dependency, pg_depend->refclassid, pg_depend->refobjid);

		/*
		 * Dependencies are traversed depth first and added to the dependencyList. By
		 * adding them depth first we make sure the dependencies are created in the right
		 * order.
		 *
		 * Dependencies we do not support the creation for are ignored and assumed to be
		 * created on the workers via a different process.
		 */

		if (pg_depend->deptype != DEPENDENCY_NORMAL)
		{
			continue;
		}

		if (IsObjectAddressInList(&dependency, *dependencyList))
		{
			continue;
		}

		if (!ShouldFollowDependency(&dependency))
		{
			continue;
		}

		/* recursion first to cause the depth first behaviour described above */
		GetDependenciesForObject(&dependency, dependencyList);

		/* palloc and copy the dependency entry to be able to add it to the list */
		dependencyPtr = palloc0(sizeof(ObjectAddress));
		*dependencyPtr = dependency;
		*dependencyList = lappend(*dependencyList, dependencyPtr);
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);
}


/*
 * IsObjectAddressInList is a helper function that can check if an ObjectAddress is
 * already in a (unsorted) list of ObjectAddress'.
 */
static bool
IsObjectAddressInList(const ObjectAddress *findAddress, List *addressList)
{
	ListCell *addressCell = NULL;
	foreach(addressCell, addressList)
	{
		ObjectAddress *currentAddress = (ObjectAddress *) lfirst(addressCell);

		/* equality check according as used in postgres for object_address_present */
		if (findAddress->classId == currentAddress->classId && findAddress->objectId ==
			currentAddress->objectId)
		{
			if (findAddress->objectSubId == currentAddress->objectSubId ||
				currentAddress->objectSubId == 0)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * ShouldFollowDependency applies a couple of rules to test if we need to traverse the
 * dependencies of this objects.
 *
 * Most important check; we nee to support the object for distribution, otherwise we do
 * not need to traverse it and its dependencies. For simplicity in the implementation we
 * actually check this as the last check.
 *
 * Other checks we do:
 *  - do not follow if the object is owned by an extension.
 *    As extensions are created separately on each worker we do not need to take into
 *    account objects that are the result of CREATE EXTENSION.
 *
 *  - do not follow objects that citus has already distributed.
 *    Objects only need to be distributed once. If citus has already distributed the
 *    object we do not follow its dependencies.
 */
static bool
ShouldFollowDependency(const ObjectAddress *toFollow)
{
	/*
	 * objects having a dependency on an extension are assumed to be created by the
	 * extension when that was created on the worker
	 */
	if (IsObjectAddressOwnedByExtension(toFollow))
	{
		return false;
	}

	/*
	 * If the object is already distributed we do not have to follow this object
	 */
	if (isObjectDistributedByAddress(toFollow))
	{
		return false;
	}

	/*
	 * looking at the type of a object to see if we should follow this dependency to
	 * create on the workers.
	 */
	switch (getObjectClass(toFollow))
	{
		case OCLASS_SCHEMA:
		{
			/* always follow */
			return true;
		}

		default:
		{
			/* unsupported type */
			return false;
		}
	}

	/*
	 * all types should have returned above, compilers complaining about a non return path
	 * indicate a bug int the above switch. Fix it there instead of adding a return here.
	 */
}


/*
 * IsObjectAddressOwnedByExtension returns whether or not the object is owned by an
 * extension. It is assumed that an object having a dependency on an extension is created
 * by that extension and therefore owned by that extension.
 */
static bool
IsObjectAddressOwnedByExtension(const ObjectAddress *target)
{
	Relation depRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;
	bool result = false;

	depRel = heap_open(DependRelationId, AccessShareLock);

	/* scan pg_depend for classid = $1 AND objid = $2 using pg_depend_depender_index */
	ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->classId));
	ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(target->objectId));
	depScan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);

	while (HeapTupleIsValid(depTup = systable_getnext(depScan)))
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);
		if (pg_depend->deptype == DEPENDENCY_EXTENSION)
		{
			result = true;
			break;
		}
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);

	return result;
}
