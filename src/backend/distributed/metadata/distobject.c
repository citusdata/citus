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
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_extension_d.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "citus_version.h"
#include "commands/extension.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/regproc.h"
#include "utils/rel.h"


static int ExecuteCommandAsSuperuser(char *query, int paramCount, Oid *paramTypes,
									 Datum *paramValues);

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

	if (ObjectExists(&address))
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
 * ObjectExists checks if an object given by its object address exists
 *
 * This is done by opening the catalog for the object and search the catalog for the
 * objects' oid. If we can find a tuple the object is existing. If no tuple is found, or
 * we don't have the information to find the tuple by its oid we assume the object does
 * not exist.
 */
bool
ObjectExists(const ObjectAddress *address)
{
	if (address == NULL)
	{
		return false;
	}

	if (is_objectclass_supported(address->classId))
	{
		HeapTuple objtup;
		Relation catalog = heap_open(address->classId, AccessShareLock);

#if PG_VERSION_NUM >= 120000
		objtup = get_catalog_object_by_oid(catalog, get_object_attnum_oid(
											   address->classId), address->objectId);
#else
		objtup = get_catalog_object_by_oid(catalog, address->objectId);
#endif
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
 * by adding appropriate entries to citus.pg_dist_object.
 *
 * The only exception is the Citus extension itself, we never mark it as distributed
 * object because that'd prevent rolling upgrades as we propagate ALTER EXTENSION
 * commands on distributed extensions.
 */
void
MarkObjectDistributed(const ObjectAddress *distAddress)
{
	int paramCount = 3;
	Oid paramTypes[3] = {
		OIDOID,
		OIDOID,
		INT4OID
	};
	Datum paramValues[3] = {
		ObjectIdGetDatum(distAddress->classId),
		ObjectIdGetDatum(distAddress->objectId),
		Int32GetDatum(distAddress->objectSubId)
	};
	int spiStatus = 0;

	char *insertQuery = "INSERT INTO citus.pg_dist_object (classid, objid, objsubid) "
						"VALUES ($1, $2, $3) ON CONFLICT DO NOTHING";

	spiStatus = ExecuteCommandAsSuperuser(insertQuery, paramCount, paramTypes,
										  paramValues);
	if (spiStatus < 0)
	{
		ereport(ERROR, (errmsg("failed to insert object into citus.pg_dist_object")));
	}
}


/*
 * CitusExtensionObject returns true if the objectAddress represents
 * the Citus extension.
 */
bool
CitusExtensionObject(const ObjectAddress *objectAddress)
{
	char *extensionName = false;

	if (objectAddress->classId != ExtensionRelationId)
	{
		return false;
	}

	extensionName = get_extension_name(objectAddress->objectId);
	if (extensionName != NULL &&
		strncasecmp(extensionName, CITUS_NAME, NAMEDATALEN) == 0)
	{
		return true;
	}

	return false;
}


/*
 * ExecuteCommandAsSuperuser executes a command via SPI as superuser. Using this
 * function (and in general SPI/SQL with superuser) should be avoided as much as
 * possible. This is to prevent any user to exploit the superuser access via
 * triggers.
 */
static int
ExecuteCommandAsSuperuser(char *query, int paramCount, Oid *paramTypes,
						  Datum *paramValues)
{
	int spiConnected = 0;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	int spiStatus = 0;
	int spiFinished = 0;

	spiConnected = SPI_connect();
	if (spiConnected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	/* make sure we have write access */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	spiStatus = SPI_execute_with_args(query, paramCount, paramTypes, paramValues,
									  NULL, false, 0);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	spiFinished = SPI_finish();
	if (spiFinished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}

	return spiStatus;
}


/*
 * UnmarkObjectDistributed removes the entry from pg_dist_object that marks this object as
 * distributed. This will prevent updates to that object to be propagated to the worker.
 */
void
UnmarkObjectDistributed(const ObjectAddress *address)
{
	int paramCount = 3;
	Oid paramTypes[3] = {
		OIDOID,
		OIDOID,
		INT4OID
	};
	Datum paramValues[3] = {
		ObjectIdGetDatum(address->classId),
		ObjectIdGetDatum(address->objectId),
		Int32GetDatum(address->objectSubId)
	};
	int spiStatus = 0;

	char *deleteQuery = "DELETE FROM citus.pg_dist_object WHERE classid = $1 AND "
						"objid = $2 AND objsubid = $3";

	spiStatus = ExecuteCommandAsSuperuser(deleteQuery, paramCount, paramTypes,
										  paramValues);
	if (spiStatus < 0)
	{
		ereport(ERROR, (errmsg("failed to delete object from citus.pg_dist_object")));
	}
}


/*
 * IsObjectDistributed returns if the object addressed is already distributed in the
 * cluster. This performs a local indexed lookup in pg_dist_object.
 */
bool
IsObjectDistributed(const ObjectAddress *address)
{
	Relation pgDistObjectRel = NULL;
	ScanKeyData key[3];
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
				Int32GetDatum(address->objectSubId));
	pgDistObjectScan = systable_beginscan(pgDistObjectRel, DistObjectPrimaryKeyIndexId(),
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
 * ClusterHasDistributedFunctionWithDistArgument returns true if there
 * is at least one distributed function in the cluster with distribution
 * argument index set.
 */
bool
ClusterHasDistributedFunctionWithDistArgument(void)
{
	bool foundDistributedFunction = false;

	SysScanDesc pgDistObjectScan = NULL;
	HeapTuple pgDistObjectTup = NULL;

	Relation pgDistObjectRel = heap_open(DistObjectRelationId(), AccessShareLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistObjectRel);

	pgDistObjectScan =
		systable_beginscan(pgDistObjectRel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		Form_pg_dist_object pg_dist_object =
			(Form_pg_dist_object) GETSTRUCT(pgDistObjectTup);

		if (pg_dist_object->classid == ProcedureRelationId)
		{
			bool distArgumentIsNull = false;
			distArgumentIsNull =
				heap_attisnull(pgDistObjectTup,
							   Anum_pg_dist_object_distribution_argument_index,
							   tupleDescriptor);

			/* we found one distributed function that has an distribution argument */
			if (!distArgumentIsNull)
			{
				foundDistributedFunction = true;

				break;
			}
		}
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);

	return foundDistributedFunction;
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
