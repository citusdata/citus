/*-------------------------------------------------------------------------
 *
 * distobject.c
 *	  Functions to interact with distributed objects by their ObjectAddress
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

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
#include "distributed/colocation_utils.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/regproc.h"
#include "utils/rel.h"


static char * CreatePgDistObjectEntryCommand(const ObjectAddress *objectAddress);
static int ExecuteCommandAsSuperuser(char *query, int paramCount, Oid *paramTypes,
									 Datum *paramValues);

PG_FUNCTION_INFO_V1(citus_unmark_object_distributed);
PG_FUNCTION_INFO_V1(master_unmark_object_distributed);


/*
 * citus_unmark_object_distributed(classid oid, objid oid, objsubid int)
 *
 * removes the entry for an object address from pg_dist_object. Only removes the entry if
 * the object does not exist anymore.
 */
Datum
citus_unmark_object_distributed(PG_FUNCTION_ARGS)
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
								  getObjectTypeDescription_compat(&address,

		                                                          /* missingOk: */ false),
								  getObjectIdentity_compat(&address,

		                                                   /* missingOk: */ false)),
						errhint("drop the object via a DROP command")));
	}

	UnmarkObjectDistributed(&address);

	PG_RETURN_VOID();
}


/*
 * master_unmark_object_distributed is a wrapper function for old UDF name.
 */
Datum
master_unmark_object_distributed(PG_FUNCTION_ARGS)
{
	return citus_unmark_object_distributed(fcinfo);
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
		Relation catalog = table_open(address->classId, AccessShareLock);

		HeapTuple objtup = get_catalog_object_by_oid(catalog, get_object_attnum_oid(
														 address->classId),
													 address->objectId);
		table_close(catalog, AccessShareLock);
		if (objtup != NULL)
		{
			return true;
		}

		return false;
	}

	return false;
}


/*
 * MarkObjectDistributed marks an object as a distributed object. Marking is done
 * by adding appropriate entries to citus.pg_dist_object and also marking the object
 * as distributed by opening a connection using current user to all of the workers
 * with metadata if object propagation is on.
 *
 * This function should be used if the user creating the given object. If you want
 * to mark dependent objects as distributed check MarkObjectDistributedViaSuperUser.
 */
void
MarkObjectDistributed(const ObjectAddress *distAddress)
{
	MarkObjectDistributedLocally(distAddress);

	if (EnableMetadataSync)
	{
		char *workerPgDistObjectUpdateCommand =
			CreatePgDistObjectEntryCommand(distAddress);
		SendCommandToWorkersWithMetadata(workerPgDistObjectUpdateCommand);
	}
}


/*
 * MarkObjectDistributedViaSuperUser marks an object as a distributed object. Marking
 * is done by adding appropriate entries to citus.pg_dist_object and also marking the
 * object as distributed by opening a connection using super user to all of the workers
 * with metadata if object propagation is on.
 *
 * This function should be used to mark dependent object as distributed. If you want
 * to mark the object you are creating please check MarkObjectDistributed.
 */
void
MarkObjectDistributedViaSuperUser(const ObjectAddress *distAddress)
{
	MarkObjectDistributedLocally(distAddress);

	if (EnableMetadataSync)
	{
		char *workerPgDistObjectUpdateCommand =
			CreatePgDistObjectEntryCommand(distAddress);
		SendCommandToWorkersWithMetadataViaSuperUser(workerPgDistObjectUpdateCommand);
	}
}


/*
 * MarkObjectDistributedLocally marks an object as a distributed object by citus.
 * Marking is done by adding appropriate entries to citus.pg_dist_object.
 *
 * This function should never be called alone, MarkObjectDistributed() or
 * MarkObjectDistributedViaSuperUser() should be called.
 */
void
MarkObjectDistributedLocally(const ObjectAddress *distAddress)
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
	char *insertQuery =
		"INSERT INTO pg_catalog.pg_dist_object (classid, objid, objsubid) "
		"VALUES ($1, $2, $3) ON CONFLICT DO NOTHING";
	int spiStatus = ExecuteCommandAsSuperuser(insertQuery, paramCount, paramTypes,
											  paramValues);
	if (spiStatus < 0)
	{
		ereport(ERROR, (errmsg("failed to insert object into citus.pg_dist_object")));
	}
}


/*
 * CreatePgDistObjectEntryCommand creates command to insert pg_dist_object tuple
 * for the given object address.
 */
static char *
CreatePgDistObjectEntryCommand(const ObjectAddress *objectAddress)
{
	/* create a list by adding the address of value to not to have warning */
	List *objectAddressList =
		list_make1((ObjectAddress *) objectAddress);
	List *distArgumetIndexList = list_make1_int(INVALID_DISTRIBUTION_ARGUMENT_INDEX);
	List *colocationIdList = list_make1_int(INVALID_COLOCATION_ID);
	List *forceDelegationList = list_make1_int(NO_FORCE_PUSHDOWN);

	char *workerPgDistObjectUpdateCommand =
		MarkObjectsDistributedCreateCommand(objectAddressList,
											distArgumetIndexList,
											colocationIdList,
											forceDelegationList);

	return workerPgDistObjectUpdateCommand;
}


/*
 * CitusExtensionObject returns true if the objectAddress represents
 * the Citus extension.
 */
bool
CitusExtensionObject(const ObjectAddress *objectAddress)
{
	if (objectAddress->classId != ExtensionRelationId)
	{
		return false;
	}

	char *extensionName = get_extension_name(objectAddress->objectId);
	if (extensionName != NULL &&
		strncasecmp(extensionName, "citus", NAMEDATALEN) == 0)
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
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;

	int spiConnected = SPI_connect();
	if (spiConnected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	/* make sure we have write access */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	int spiStatus = SPI_execute_with_args(query, paramCount, paramTypes, paramValues,
										  NULL, false, 0);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	int spiFinished = SPI_finish();
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

	char *deleteQuery = "DELETE FROM pg_catalog.pg_dist_object WHERE classid = $1 AND "
						"objid = $2 AND objsubid = $3";

	int spiStatus = ExecuteCommandAsSuperuser(deleteQuery, paramCount, paramTypes,
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
	ScanKeyData key[3];
	bool result = false;

	Relation pgDistObjectRel = table_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = $1 AND objid = $2 AND objsubid = $3 via index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->classId));
	ScanKeyInit(&key[1], Anum_pg_dist_object_objid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(address->objectId));
	ScanKeyInit(&key[2], Anum_pg_dist_object_objsubid, BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(address->objectSubId));
	SysScanDesc pgDistObjectScan = systable_beginscan(pgDistObjectRel,
													  DistObjectPrimaryKeyIndexId(),
													  true, NULL, 3, key);

	HeapTuple pgDistObjectTup = systable_getnext(pgDistObjectScan);
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
	HeapTuple pgDistObjectTup = NULL;
	List *objectAddressList = NIL;

	Relation pgDistObjectRel = table_open(DistObjectRelationId(), AccessShareLock);
	SysScanDesc pgDistObjectScan = systable_beginscan(pgDistObjectRel, InvalidOid, false,
													  NULL, 0,
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


/*
 * GetRoleSpecObjectForUser creates a RoleSpec object for the given roleOid.
 */
RoleSpec *
GetRoleSpecObjectForUser(Oid roleOid)
{
	RoleSpec *roleSpec = makeNode(RoleSpec);
	roleSpec->roletype = OidIsValid(roleOid) ? ROLESPEC_CSTRING : ROLESPEC_PUBLIC;
	roleSpec->rolename = OidIsValid(roleOid) ? GetUserNameFromId(roleOid, false) : NULL;
	roleSpec->location = -1;

	return roleSpec;
}


/*
 * UpdateDistributedObjectColocationId gets an old and a new colocationId
 * and updates the colocationId of all tuples in citus.pg_dist_object which
 * have the old colocationId to the new colocationId.
 */
void
UpdateDistributedObjectColocationId(uint32 oldColocationId,
									uint32 newColocationId)
{
	const bool indexOK = false;
	ScanKeyData scanKey[1];
	Relation pgDistObjectRel = table_open(DistObjectRelationId(),
										  RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistObjectRel);

	/* scan pg_dist_object for colocationId equal to old colocationId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_object_colocationid,
				BTEqualStrategyNumber,
				F_INT4EQ, UInt32GetDatum(oldColocationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistObjectRel,
													InvalidOid,
													indexOK,
													NULL, 1, scanKey);
	HeapTuple heapTuple;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum values[Natts_pg_dist_object];
		bool isnull[Natts_pg_dist_object];
		bool replace[Natts_pg_dist_object];

		memset(replace, 0, sizeof(replace));

		replace[Anum_pg_dist_object_colocationid - 1] = true;

		/* update the colocationId to the new one */
		values[Anum_pg_dist_object_colocationid - 1] = UInt32GetDatum(newColocationId);

		isnull[Anum_pg_dist_object_colocationid - 1] = false;

		heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
									  replace);

		CatalogTupleUpdate(pgDistObjectRel, &heapTuple->t_self, heapTuple);
		CitusInvalidateRelcacheByRelid(DistObjectRelationId());
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistObjectRel, NoLock);
	CommandCounterIncrement();
}
