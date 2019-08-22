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
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

#include "distributed/dist_catalog/dependency.h"
#include "distributed/dist_catalog/distobject.h"

typedef struct ObjectAddressCollector
{
	List *dependencyList;
	HTAB *dependencySet;
} ObjectAddressCollector;


static bool SupportedDependencyByCitus(const ObjectAddress *address);
static bool IsObjectAddressCollected(const ObjectAddress *findAddress,
									 ObjectAddressCollector *collector);
static bool IsObjectAddressOwnedByExtension(const ObjectAddress *target);

static void recurse_pg_depend(const ObjectAddress *target,
							  List * (*expand)(void *context, const
											   ObjectAddress *target),
							  bool (*follow)(void *context, Form_pg_depend row),
							  void (*apply)(void *context, Form_pg_depend row),
							  void *context);
static void recurse_pg_depend_iterate(Form_pg_depend pg_depend,
									  List * (*expand)(void *context, const
													   ObjectAddress *target),
									  bool (*follow)(void *context, Form_pg_depend row),
									  void (*apply)(void *context, Form_pg_depend row),
									  void *context);
static bool FollowAllSupportedDependencies(void *context, Form_pg_depend pg_depend);
static bool FollowNewSupportedDependencies(void *context, Form_pg_depend pg_depend);
static void ApplyAddToDependencyList(void *context, Form_pg_depend pg_depend);

static void InitObjectAddressCollector(ObjectAddressCollector *collector);
static void CollectObjectAddress(ObjectAddressCollector *collector, const
								 ObjectAddress *address);

static void
InitObjectAddressCollector(ObjectAddressCollector *collector)
{
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ObjectAddress);
	info.entrysize = sizeof(ObjectAddress);
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	collector->dependencySet = hash_create("dependency set", 128, &info, hashFlags);
}


static void
CollectObjectAddress(ObjectAddressCollector *collector, const ObjectAddress *collect)
{
	ObjectAddress *address = NULL;
	bool found = false;

	/* add to set */
	address = (ObjectAddress *) hash_search(collector->dependencySet, collect,
											HASH_ENTER, &found);

	if (!found)
	{
		/* copy object address in */
		*address = *collect;
	}

	/* add to list*/
	collector->dependencyList = lappend(collector->dependencyList, address);
}


/*
 * GetDependenciesForObject returns a list of ObjectAddesses to be created in order
 * before the target object could safely be created on a worker. Some of the object might
 * already be created on a worker. It should be created in an idempotent way.
 */
List *
GetDependenciesForObject(const ObjectAddress *target)
{
	ObjectAddressCollector collector = { 0 };

	InitObjectAddressCollector(&collector);

	recurse_pg_depend(target,
					  NULL,
					  &FollowNewSupportedDependencies,
					  &ApplyAddToDependencyList,
					  &collector);

	return collector.dependencyList;
}


/*
 * IsObjectAddressCollected is a helper function that can check if an ObjectAddress is
 * already in a (unsorted) list of ObjectAddresses
 */
static bool
IsObjectAddressCollected(const ObjectAddress *findAddress,
						 ObjectAddressCollector *collector)
{
	bool found = false;

	/* add to set */
	hash_search(collector->dependencySet, findAddress, HASH_FIND, &found);

	return found;
}


/*
 * SupportedDependencyByCitus returns whether citus has support to distribute the object
 * addressed.
 */
static bool
SupportedDependencyByCitus(const ObjectAddress *address)
{
	/*
	 * looking at the type of a object to see if we know how to create the object on the
	 * workers.
	 */
	switch (getObjectClass(address))
	{
		case OCLASS_SCHEMA:
		{
			return true;
		}

		default:
		{
			/* unsupported type */
			return false;
		}
	}
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


/*
 * OrderObjectAddressListInDependencyOrder given a list of ObjectAddresses return a new
 * list of the same ObjectAddresses ordered on dependency order where dependencies
 * precedes the corresponding object in the list.
 *
 * The algortihm traveses pg_depend in a depth first order starting at the first object in
 * the provided list. By traversing depth first it will put the first dependency at the
 * head of the list with dependencies depending on them later.
 *
 * If the object is already in the list it is skipped for traversal. This happens when an
 * object was already added to the target list before it occurred in the input list.
 */
List *
OrderObjectAddressListInDependencyOrder(List *objectAddressList)
{
	ObjectAddressCollector collector = { 0 };
	ListCell *ojectAddressCell = NULL;

	InitObjectAddressCollector(&collector);

	foreach(ojectAddressCell, objectAddressList)
	{
		ObjectAddress *objectAddress = (ObjectAddress *) lfirst(ojectAddressCell);

		if (IsObjectAddressCollected(objectAddress, &collector))
		{
			/* skip objects that are already ordered */
			continue;
		}

		recurse_pg_depend(objectAddress,
						  NULL,
						  &FollowAllSupportedDependencies,
						  &ApplyAddToDependencyList,
						  &collector);

		CollectObjectAddress(&collector, objectAddress);
	}

	return collector.dependencyList;
}


/*
 * recurse_pg_depend recursively visits pg_depend entries.
 *
 * `expand` allows based on the target ObjectAddress to generate extra entries for ease of
 * traversal.
 *
 * Starting from the target ObjectAddress. For every existing and generated entry the
 * `follow` function will be called. When `follow` returns true it will recursively visit
 * the dependencies for that object. recurse_pg_depend will visit therefore all pg_depend
 * entries.
 *
 * Visiting will happen in depth first order, which is useful to create or sorted lists of
 * dependencies to create.
 *
 * For all pg_depend entries that should be visited the apply function will be called.
 * This function is designed to be the mutating function for the context being passed.
 * Although nothing prevents the follow function to also mutate the context.
 *
 *  - follow will be called on the way down, so the invocation order is top to bottom of
 *    the dependency tree
 *  - apply is called on the way back, so the invocation order is bottom to top. Apply is
 *    not called for entries for which follow has returned false.
 */
static void
recurse_pg_depend(const ObjectAddress *target,
				  List * (*expand)(void *context, const ObjectAddress *target),
				  bool (*follow)(void *context, Form_pg_depend row),
				  void (*apply)(void *context, Form_pg_depend row),
				  void *context)
{
	Relation depRel = NULL;
	ScanKeyData key[2] = { 0 };
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;

	/********************
	* first apply on expanded entries of pg_depend
	********************/
	if (expand != NULL)
	{
		List *expandedEntries = NIL;
		ListCell *expandedEntryCell = NULL;

		expandedEntries = expand(context, target);
		foreach(expandedEntryCell, expandedEntries)
		{
			Form_pg_depend pg_depend = (Form_pg_depend) lfirst(expandedEntryCell);
			recurse_pg_depend_iterate(pg_depend, expand, follow, apply, context);
		}
	}

	/********************
	* secondly iterate the actual pg_depend catalog
	********************/
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
		recurse_pg_depend_iterate(pg_depend, expand, follow, apply, context);
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);
}


/*
 * recurse_pg_depend_iterate is called for every real and expanded pg_depend entries for
 * recurse_pg_depend. It calls follow for the entry and if it should follow it recurses
 * back to recurse_pg_depend with a new target, all other arguments are passed verbatim.
 *
 * after we have recursed we call the apply function
 */
static void
recurse_pg_depend_iterate(Form_pg_depend pg_depend,
						  List * (*expand)(void *context, const ObjectAddress *target),
						  bool (*follow)(void *context, Form_pg_depend row),
						  void (*apply)(void *context, Form_pg_depend row),
						  void *context)
{
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

	if (follow == NULL || !follow(context, pg_depend))
	{
		/* skip all pg_depend entries the user didn't want to follow */
		return;
	}

	/*
	 * recurse depth first, this makes sure we call apply for the deepest dependency
	 * first.
	 */
	recurse_pg_depend(&address, expand, follow, apply, context);

	/* now apply changes for current entry */
	if (apply != NULL)
	{
		apply(context, pg_depend);
	}
}


/*
 * FollowNewSupportedDependencies applies filters on pg_depend entries to follow all
 * objects which should be distributed before the root object can safely be created.
 */
static bool
FollowNewSupportedDependencies(void *context, Form_pg_depend pg_depend)
{
	ObjectAddressCollector *collector = (ObjectAddressCollector *) context;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

	/*
	 *  Distirbute only normal dependencies, other dependencies are internal dependencies
	 *  and managed by postgres
	 */
	if (pg_depend->deptype != DEPENDENCY_NORMAL)
	{
		return false;
	}

	/*
	 * We can only distribute dependencies that citus knows how to distribute
	 */
	if (!SupportedDependencyByCitus(&address))
	{
		return false;
	}

	/*
	 * If the object is already in our dependency list we do not have to follow any
	 * further
	 */
	if (IsObjectAddressCollected(&address, collector))
	{
		return false;
	}

	/*
	 * If the object is already distributed it is not a `new` object that needs to be
	 * distributed before we create a dependant object
	 */
	if (IsObjectDistributed(&address))
	{
		return false;
	}

	/*
	 * Objects owned by an extension are assumed to be created on the workers by creating
	 * the extension in the cluster
	 */
	if (IsObjectAddressOwnedByExtension(&address))
	{
		return false;
	}

	return true;
}


/*
 * FollowAllSupportedDependencies applies filters on pg_depend entries to follow the
 * dependency tree of objects in depth first order. We will visit all supported objects.
 * This is used to sort a list of dependencies in dependency order.
 */
static bool
FollowAllSupportedDependencies(void *context, Form_pg_depend pg_depend)
{
	ObjectAddressCollector *collector = (ObjectAddressCollector *) context;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

	/*
	 *  Distirbute only normal dependencies, other dependencies are internal dependencies
	 *  and managed by postgres
	 */
	if (pg_depend->deptype != DEPENDENCY_NORMAL)
	{
		return false;
	}

	/*
	 * We can only distribute dependencies that citus knows how to distribute
	 */
	if (!SupportedDependencyByCitus(&address))
	{
		return false;
	}

	/*
	 * If the object is already in our dependency list we do not have to follow any
	 * further
	 */
	if (IsObjectAddressCollected(&address, collector))
	{
		return false;
	}

	/*
	 * Objects owned by an extension are assumed to be created on the workers by creating
	 * the extension in the cluster
	 */
	if (IsObjectAddressOwnedByExtension(&address))
	{
		return false;
	}

	return true;
}


/*
 * ApplyAddToDependencyList is an apply function for recurse_pg_depend that will append
 * all the ObjectAddresses for pg_depend entries to the context. The context here is
 * assumed to be a (List **) to the location where all ObjectAddresses will be collected.
 */
static void
ApplyAddToDependencyList(void *context, Form_pg_depend pg_depend)
{
	ObjectAddressCollector *collector = (ObjectAddressCollector *) context;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

	CollectObjectAddress(collector, &address);
}
