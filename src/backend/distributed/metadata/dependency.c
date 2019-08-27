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
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

/*
 * ObjectAddressCollector keeps track of collected ObjectAddresses. This can be used
 * together with recurse_pg_depend.
 *
 * We keep two different datastructures for the following reasons
 *  - A List ordered by insert/collect order
 *  - A Set to quickly O(1) check if an ObjectAddress has already been collected
 */
typedef struct ObjectAddressCollector
{
	List *dependencyList;
	HTAB *dependencySet;
} ObjectAddressCollector;


/* forward declarations for functions to interact with the ObjectAddressCollector */
static void InitObjectAddressCollector(ObjectAddressCollector *collector);
static void CollectObjectAddress(ObjectAddressCollector *collector, const
								 ObjectAddress *address);
static bool IsObjectAddressCollected(const ObjectAddress *findAddress,
									 ObjectAddressCollector *collector);

/* forward declaration of functions that recurse pg_depend */
static void recurse_pg_depend(const ObjectAddress *target,
							  List * (*expand)(void *context, const
											   ObjectAddress *target),
							  bool (*follow)(void *context, Form_pg_depend row),
							  void (*apply)(void *context, Form_pg_depend row),
							  void *context);
static bool FollowAllSupportedDependencies(void *context, Form_pg_depend pg_depend);
static bool FollowNewSupportedDependencies(void *context, Form_pg_depend pg_depend);
static void ApplyAddToDependencyList(void *context, Form_pg_depend pg_depend);
static List * EpxandCitusSupportedTypes(void *context, const ObjectAddress *target);

/* forward declaration of support functions to decide what to follow */
static bool SupportedDependencyByCitus(const ObjectAddress *address);
static bool IsObjectAddressOwnedByExtension(const ObjectAddress *target);


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
					  &EpxandCitusSupportedTypes,
					  &FollowNewSupportedDependencies,
					  &ApplyAddToDependencyList,
					  &collector);

	return collector.dependencyList;
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
						  &EpxandCitusSupportedTypes,
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
	ScanKeyData key[2];
	SysScanDesc depScan = NULL;
	HeapTuple depTup = NULL;
	List *pgDependEntries = NIL;
	ListCell *pgDependCell = NULL;

	/*
	 * iterate the actual pg_depend catalog
	 */
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
		Form_pg_depend pg_depend_copy = palloc0(sizeof(FormData_pg_depend));

		*pg_depend_copy = *pg_depend;

		pgDependEntries = lappend(pgDependEntries, pg_depend_copy);
	}

	systable_endscan(depScan);
	relation_close(depRel, AccessShareLock);

	/*
	 * concat expended entries if applicable
	 */
	if (expand != NULL)
	{
		List *expandedEntries = NIL;

		expandedEntries = expand(context, target);
		pgDependEntries = list_concat(pgDependEntries, expandedEntries);
	}

	/*
	 * Iterate all entries and recurse depth first
	 */
	foreach(pgDependCell, pgDependEntries)
	{
		Form_pg_depend pg_depend = (Form_pg_depend) lfirst(pgDependCell);
		ObjectAddress address = { 0 };
		ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

		if (follow == NULL || !follow(context, pg_depend))
		{
			/* skip all pg_depend entries the user didn't want to follow */
			continue;
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
}


/*
 * InitObjectAddressCollector takes a pointer to an already allocated (possibly stack)
 * ObjectAddressCollector struct. It makes sure this struct is ready to be used for object
 * collection.
 *
 * If an already initialized collector is passed the collector will be cleared from its
 * contents to be reused.
 */
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
	collector->dependencyList = NULL;
}


/*
 * CollectObjectAddress adds an ObjectAddress to the collector.
 */
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

		case OCLASS_TYPE:
		{
			switch (get_typtype(address->objectId))
			{
				case TYPTYPE_ENUM:
				case TYPTYPE_COMPOSITE:
				{
					return true;
				}

				case TYPTYPE_BASE:
				{
					/*
					 * array types should be followed but not created, as they get created
					 * by the original type.
					 */
					return type_is_array(address->objectId);
				}

				default:
				{
					/* type not supported */
					return false;
				}
			}

			/*
			 * should be unreachable, break here is to make sure the function has a path
			 * without return, instead of falling through to the next block */
			break;
		}

		case OCLASS_CLASS:
		{
			/*
			 * composite types have a reference to a relation of composite type, we need
			 * to follow those to get the dependencies of type fields.
			 */
			if (get_rel_relkind(address->objectId) == RELKIND_COMPOSITE_TYPE)
			{
				return true;
			}

			return false;
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
	ScanKeyData key[2];
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
	heap_close(depRel, AccessShareLock);

	return result;
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
 * ApplyAddToDependencyList is an apply function for recurse_pg_depend that will collect
 * all the ObjectAddresses for pg_depend entries to the context. The context here is
 * assumed to be a (ObjectAddressCollector *) to the location where all ObjectAddresses
 * will be collected.
 */
static void
ApplyAddToDependencyList(void *context, Form_pg_depend pg_depend)
{
	ObjectAddressCollector *collector = (ObjectAddressCollector *) context;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, pg_depend->refclassid, pg_depend->refobjid);

	CollectObjectAddress(collector, &address);
}


/*
 * EpxandCitusSupportedTypes base on supported types by citus we might want to expand
 * the list of objects to visit in pg_depend.
 *
 * An example where we want to expand is for types. Their dependencies are not captured
 * with an entry in pg_depend from their object address, but by the object address of the
 * relation describing the type.
 */
static List *
EpxandCitusSupportedTypes(void *context, const ObjectAddress *target)
{
	List *result = NIL;

	switch (target->classId)
	{
		case TypeRelationId:
		{
			/*
			 * types depending on other types are not captured in pg_depend, instead they
			 * are described with their dependencies by the relation that describes the
			 * composite type.
			 */
			if (get_typtype(target->objectId) == TYPTYPE_COMPOSITE)
			{
				Form_pg_depend dependency = palloc0(sizeof(FormData_pg_depend));
				dependency->classid = target->classId;
				dependency->objid = target->objectId;
				dependency->objsubid = target->objectSubId;

				/* add outward edge to the type's relation */
				dependency->refclassid = RelationRelationId;
				dependency->refobjid = get_typ_typrelid(target->objectId);
				dependency->refobjsubid = 0;

				dependency->deptype = DEPENDENCY_NORMAL;

				result = lappend(result, dependency);
			}

			/*
			 * array types don't have a normal dependency on their element type, instead
			 * their dependency is an internal one. We can't follow interal dependencies
			 * as that would cause a cyclic dependency on others, instead we expand here
			 * to follow the dependency on the element type.
			 */
			if (type_is_array(target->objectId))
			{
				Form_pg_depend dependency = palloc0(sizeof(FormData_pg_depend));
				dependency->classid = target->classId;
				dependency->objid = target->objectId;
				dependency->objsubid = target->objectSubId;

				/* add outward edge to the element type */
				dependency->refclassid = TypeRelationId;
				dependency->refobjid = get_element_type(target->objectId);
				dependency->refobjsubid = 0;

				dependency->deptype = DEPENDENCY_NORMAL;

				result = lappend(result, dependency);
			}

			break;
		}

		default:
		{
			/* no expansion for unsupported types */
			break;
		}
	}
	return result;
}
