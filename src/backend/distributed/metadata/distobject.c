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

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension_d.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_type.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"

#include "citus_version.h"
#include "pg_version_constants.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata/pg_dist_object.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"

static char * CreatePgDistObjectEntryCommand(const ObjectAddress *objectAddress,
											 char *objectName);
static int ExecuteCommandAsSuperuser(char *query, int paramCount, Oid *paramTypes,
									 Datum *paramValues);
static bool IsObjectDistributed(const ObjectAddress *address);

PG_FUNCTION_INFO_V1(mark_object_distributed);
PG_FUNCTION_INFO_V1(citus_unmark_object_distributed);
PG_FUNCTION_INFO_V1(master_unmark_object_distributed);


/*
 * mark_object_distributed adds an object to pg_dist_object
 * in all of the nodes, for the connections to the other nodes this function
 * uses the user passed.
 */
Datum
mark_object_distributed(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureSuperUser();

	Oid classId = PG_GETARG_OID(0);
	text *objectNameText = PG_GETARG_TEXT_P(1);
	char *objectName = text_to_cstring(objectNameText);
	Oid objectId = PG_GETARG_OID(2);
	ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*objectAddress, classId, objectId);
	text *connectionUserText = PG_GETARG_TEXT_P(3);
	char *connectionUser = text_to_cstring(connectionUserText);

	/*
	 * This function is called when a query is run from a Citus non-main database.
	 * We need to insert into local pg_dist_object over a connection to make sure
	 * 2PC still works.
	 */
	bool useConnectionForLocalQuery = true;
	MarkObjectDistributedWithName(objectAddress, objectName, useConnectionForLocalQuery,
								  connectionUser);
	PG_RETURN_VOID();
}


/*
 * citus_unmark_object_distributed(classid oid, objid oid, objsubid int,checkobjectexistence bool)
 *
 * Removes the entry for an object address from pg_dist_object. If checkobjectexistence is true,
 * throws an error if the object still exists.
 */
Datum
citus_unmark_object_distributed(PG_FUNCTION_ARGS)
{
	Oid classid = PG_GETARG_OID(0);
	Oid objid = PG_GETARG_OID(1);
	int32 objsubid = PG_GETARG_INT32(2);
	bool checkObjectExistence = true;
	if (!PG_ARGISNULL(3))
	{
		checkObjectExistence = PG_GETARG_BOOL(3);
	}


	ObjectAddress address = { 0 };
	ObjectAddressSubSet(address, classid, objid, objsubid);

	if (!IsObjectDistributed(&address))
	{
		/* if the object is not distributed there is no need to unmark it */
		PG_RETURN_VOID();
	}

	if (checkObjectExistence && ObjectExists(&address))
	{
		ereport(ERROR, (errmsg("object still exists"),
						errdetail("the %s \"%s\" still exists",
								  getObjectTypeDescription(&address,

		                                                   /* missingOk: */ false),
								  getObjectIdentity(&address,

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
 * as distributed by opening a connection using current user to all remote nodes
 * with metadata if object propagation is on.
 *
 * This function should be used if the user creating the given object. If you want
 * to mark dependent objects as distributed check MarkObjectDistributedViaSuperUser.
 */
void
MarkObjectDistributed(const ObjectAddress *distAddress)
{
	bool useConnectionForLocalQuery = false;
	MarkObjectDistributedWithName(distAddress, "", useConnectionForLocalQuery,
								  CurrentUserName());
}


/*
 * MarkObjectDistributedWithName marks an object as a distributed object.
 * Same as MarkObjectDistributed but this function also allows passing an objectName
 * that is used in case the object does not exists for the current transaction.
 */
void
MarkObjectDistributedWithName(const ObjectAddress *distAddress, char *objectName,
							  bool useConnectionForLocalQuery, char *connectionUser)
{
	if (!CitusHasBeenLoaded())
	{
		elog(ERROR, "Cannot mark object distributed because Citus has not been loaded.");
	}

	/*
	 * When a query is run from a Citus non-main database we need to insert into pg_dist_object
	 * over a connection to make sure 2PC still works.
	 */
	if (useConnectionForLocalQuery)
	{
		StringInfo insertQuery = makeStringInfo();
		appendStringInfo(insertQuery,
						 "INSERT INTO pg_catalog.pg_dist_object (classid, objid, objsubid)"
						 "VALUES (%d, %d, %d) ON CONFLICT DO NOTHING",
						 distAddress->classId, distAddress->objectId,
						 distAddress->objectSubId);
		SendCommandToWorker(LocalHostName, PostPortNumber, insertQuery->data);
	}
	else
	{
		MarkObjectDistributedLocally(distAddress);
	}

	if (EnableMetadataSync)
	{
		char *workerPgDistObjectUpdateCommand =
			CreatePgDistObjectEntryCommand(distAddress, objectName);
		SendCommandToRemoteMetadataNodesParams(workerPgDistObjectUpdateCommand,
											   connectionUser, 0, NULL, NULL);
	}
}


/*
 * MarkObjectDistributedViaSuperUser marks an object as a distributed object. Marking
 * is done by adding appropriate entries to citus.pg_dist_object and also marking the
 * object as distributed by opening a connection using super user to all remote nodes
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
			CreatePgDistObjectEntryCommand(distAddress, "");
		SendCommandToRemoteNodesWithMetadataViaSuperUser(workerPgDistObjectUpdateCommand);
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
 * ShouldMarkRelationDistributed is a helper function that
 * decides whether the input relation should be marked as distributed.
 */
bool
ShouldMarkRelationDistributed(Oid relationId)
{
	if (!EnableMetadataSync)
	{
		/*
		 * Just in case anything goes wrong, we should still be able
		 * to continue to the version upgrade.
		 */
		return false;
	}

	ObjectAddress *relationAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*relationAddress, RelationRelationId, relationId);

	bool pgObject = (relationId < FirstNormalObjectId);
	bool isObjectSupported = SupportedDependencyByCitus(relationAddress);
	bool ownedByExtension = IsTableOwnedByExtension(relationId);
	bool alreadyDistributed = IsObjectDistributed(relationAddress);
	bool hasUnsupportedDependency =
		DeferErrorIfAnyObjectHasUnsupportedDependency(list_make1(relationAddress)) !=
		NULL;
	bool hasCircularDependency =
		DeferErrorIfCircularDependencyExists(relationAddress) != NULL;

	/*
	 * pgObject: Citus never marks pg objects as distributed
	 * isObjectSupported: Citus does not support propagation of some objects
	 * ownedByExtension: let extensions manage its own objects
	 * alreadyDistributed: most likely via earlier versions
	 * hasUnsupportedDependency: Citus doesn't know how to distribute its dependencies
	 * hasCircularDependency: Citus cannot handle circular dependencies
	 */
	if (pgObject || !isObjectSupported || ownedByExtension || alreadyDistributed ||
		hasUnsupportedDependency || hasCircularDependency)
	{
		return false;
	}

	return true;
}


/*
 * CreatePgDistObjectEntryCommand creates command to insert pg_dist_object tuple
 * for the given object address.
 */
static char *
CreatePgDistObjectEntryCommand(const ObjectAddress *objectAddress, char *objectName)
{
	/* create a list by adding the address of value to not to have warning */
	List *objectAddressList =
		list_make1((ObjectAddress *) objectAddress);

	/* names also require a list so we create a nested list here */
	List *objectNameList = list_make1(list_make1((char *) objectName));
	List *distArgumetIndexList = list_make1_int(INVALID_DISTRIBUTION_ARGUMENT_INDEX);
	List *colocationIdList = list_make1_int(INVALID_COLOCATION_ID);
	List *forceDelegationList = list_make1_int(NO_FORCE_PUSHDOWN);

	char *workerPgDistObjectUpdateCommand =
		MarkObjectsDistributedCreateCommand(objectAddressList,
											objectNameList,
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
 * UnmarkNodeWideObjectsDistributed deletes pg_dist_object records
 * for all distributed objects in given Drop stmt node.
 *
 * Today we only expect DropRoleStmt and DropdbStmt to get here.
 */
void
UnmarkNodeWideObjectsDistributed(Node *node)
{
	if (IsA(node, DropRoleStmt))
	{
		DropRoleStmt *stmt = castNode(DropRoleStmt, node);
		List *allDropRoles = stmt->roles;

		List *distributedDropRoles = FilterDistributedRoles(allDropRoles);
		if (list_length(distributedDropRoles) > 0)
		{
			UnmarkRolesDistributed(distributedDropRoles);
		}
	}
	else if (IsA(node, DropdbStmt))
	{
		DropdbStmt *stmt = castNode(DropdbStmt, node);
		char *dbName = stmt->dbname;

		Oid dbOid = get_database_oid(dbName, stmt->missing_ok);
		ObjectAddress *dbObjectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*dbObjectAddress, DatabaseRelationId, dbOid);
		if (IsAnyObjectDistributed(list_make1(dbObjectAddress)))
		{
			UnmarkObjectDistributed(dbObjectAddress);
		}
	}
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
static bool
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
 * IsAnyObjectDistributed iteratively calls IsObjectDistributed for given addresses to
 * determine if any object is distributed.
 */
bool
IsAnyObjectDistributed(const List *addresses)
{
	ObjectAddress *address = NULL;
	foreach_ptr(address, addresses)
	{
		if (IsObjectDistributed(address))
		{
			return true;
		}
	}

	return false;
}


/*
 * IsAnyObjectDistributedIgnoreObjectSubId determines if any of the given
 * addresses are distributed, using IsObjectDistributed(). It disregards
 * the object sub-id field of an address, so this is saved and restored
 * before and after each call to IsObjectDistributed(). It is used in
 * situations where an address by sub object id is not distributed, but
 * the same address by object id is distributed; for example, the address
 * of a column of a distributed table.
 */
bool
IsAnyObjectDistributedIgnoreObjectSubId(const List *addresses)
{
	ObjectAddress *address = NULL;
	bool isDistributed = false;
	foreach_ptr(address, addresses)
	{
		int32 savedObjectSubId = address->objectSubId;
		address->objectSubId = 0;
		isDistributed = IsObjectDistributed(address);
		address->objectSubId = savedObjectSubId;

		if (isDistributed)
		{
			break;
		}
	}

	return isDistributed;
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
				F_INT4EQ, Int32GetDatum(oldColocationId));

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


/*
 * DistributedFunctionList returns the list of ObjectAddress-es of all the
 * distributed functions found in pg_dist_object
 */
List *
DistributedFunctionList(void)
{
	List *distributedFunctionList = NIL;

	ScanKeyData key[1];
	Relation pgDistObjectRel = table_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = ProcedureRelationId via index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ProcedureRelationId));
	SysScanDesc pgDistObjectScan = systable_beginscan(pgDistObjectRel,
													  DistObjectPrimaryKeyIndexId(),
													  true, NULL, 1, key);

	HeapTuple pgDistObjectTup = NULL;
	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		Form_pg_dist_object pg_dist_object =
			(Form_pg_dist_object) GETSTRUCT(pgDistObjectTup);

		ObjectAddress *functionAddress = palloc0(sizeof(ObjectAddress));
		functionAddress->classId = ProcedureRelationId;
		functionAddress->objectId = pg_dist_object->objid;
		functionAddress->objectSubId = pg_dist_object->objsubid;
		distributedFunctionList = lappend(distributedFunctionList, functionAddress);
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);
	return distributedFunctionList;
}


/*
 * DistributedSequenceList returns the list of ObjectAddress-es of all the
 * distributed sequences found in pg_dist_object
 */
List *
DistributedSequenceList(void)
{
	List *distributedSequenceList = NIL;

	ScanKeyData key[1];
	Relation pgDistObjectRel = table_open(DistObjectRelationId(), AccessShareLock);

	/* scan pg_dist_object for classid = RelationRelationId via index */
	ScanKeyInit(&key[0], Anum_pg_dist_object_classid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	SysScanDesc pgDistObjectScan = systable_beginscan(pgDistObjectRel,
													  DistObjectPrimaryKeyIndexId(),
													  true, NULL, 1, key);

	HeapTuple pgDistObjectTup = NULL;
	while (HeapTupleIsValid(pgDistObjectTup = systable_getnext(pgDistObjectScan)))
	{
		Form_pg_dist_object pg_dist_object =
			(Form_pg_dist_object) GETSTRUCT(pgDistObjectTup);

		if (get_rel_relkind(pg_dist_object->objid) == RELKIND_SEQUENCE)
		{
			ObjectAddress *sequenceAddress = palloc0(sizeof(ObjectAddress));
			sequenceAddress->classId = RelationRelationId;
			sequenceAddress->objectId = pg_dist_object->objid;
			sequenceAddress->objectSubId = pg_dist_object->objsubid;
			distributedSequenceList = lappend(distributedSequenceList, sequenceAddress);
		}
	}

	systable_endscan(pgDistObjectScan);
	relation_close(pgDistObjectRel, AccessShareLock);
	return distributedSequenceList;
}
