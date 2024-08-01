/*-------------------------------------------------------------------------
 *
 * test/src/dependency.c
 *
 * This file contains functions to exercise dependency resolution for objects.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "c.h"
#include "fmgr.h"

#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata_cache.h"
#include "distributed/tuplestore.h"


PG_FUNCTION_INFO_V1(citus_get_all_dependencies_for_object);
PG_FUNCTION_INFO_V1(citus_get_dependencies_for_object);


/*
 * citus_get_all_dependencies_for_object(classid oid, objid oid, objsubid int)
 *
 * citus_get_all_dependencies_for_object gets an object and returns all of its
 * dependencies irrespective of whether the dependencies are already distributed
 * or not.
 *
 * This is to emulate what Citus would qualify as dependency when adding a new
 * node.
 */
Datum
citus_get_all_dependencies_for_object(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid classid = PG_GETARG_OID(0);
	Oid objid = PG_GETARG_OID(1);
	int32 objsubid = PG_GETARG_INT32(2);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ObjectAddress address = { 0 };
	ObjectAddressSubSet(address, classid, objid, objsubid);

	List *dependencies = GetAllSupportedDependenciesForObject(&address);
	ObjectAddress *dependency = NULL;
	foreach_declared_ptr(dependency, dependencies)
	{
		Datum values[3];
		bool isNulls[3];

		memset(values, 0, sizeof(values));
		memset(isNulls, 0, sizeof(isNulls));

		values[0] = ObjectIdGetDatum(dependency->classId);
		values[1] = ObjectIdGetDatum(dependency->objectId);
		values[2] = Int32GetDatum(dependency->objectSubId);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}


/*
 * citus_get_dependencies_for_object(classid oid, objid oid, objsubid int)
 *
 * citus_get_dependencies_for_object gets an object and returns all of its
 * dependencies that are not already distributed.
 *
 * This is to emulate what Citus would qualify as dependency when creating
 * a new object.
 */
Datum
citus_get_dependencies_for_object(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid classid = PG_GETARG_OID(0);
	Oid objid = PG_GETARG_OID(1);
	int32 objsubid = PG_GETARG_INT32(2);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ObjectAddress address = { 0 };
	ObjectAddressSubSet(address, classid, objid, objsubid);

	List *dependencies = GetDependenciesForObject(&address);
	ObjectAddress *dependency = NULL;
	foreach_declared_ptr(dependency, dependencies)
	{
		Datum values[3];
		bool isNulls[3];

		memset(values, 0, sizeof(values));
		memset(isNulls, 0, sizeof(isNulls));

		values[0] = ObjectIdGetDatum(dependency->classId);
		values[1] = ObjectIdGetDatum(dependency->objectId);
		values[2] = Int32GetDatum(dependency->objectSubId);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}
