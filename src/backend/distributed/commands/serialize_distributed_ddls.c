/*-------------------------------------------------------------------------
 *
 * serialize_distributed_ddls.c
 *
 * This file contains functions for serializing distributed DDLs.
 *
 * If you're adding support for serializing a new DDL, you should
 * extend the following functions to support the new object class:
 *   AcquireCitusAdvisoryObjectClassLockGetOid()
 *   AcquireCitusAdvisoryObjectClassLockCheckPrivileges()
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_database_d.h"
#include "commands/dbcommands.h"
#include "storage/lock.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/adaptive_executor.h"
#include "distributed/argutils.h"
#include "distributed/commands/serialize_distributed_ddls.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/resource_lock.h"


PG_FUNCTION_INFO_V1(citus_internal_acquire_citus_advisory_object_class_lock);


static void SerializeDistributedDDLsOnObjectClassInternal(ObjectClass objectClass,
														  char *qualifiedObjectName);
static char * AcquireCitusAdvisoryObjectClassLockCommand(ObjectClass objectClass,
														 char *qualifiedObjectName);
static void AcquireCitusAdvisoryObjectClassLock(ObjectClass objectClass,
												char *qualifiedObjectName);
static Oid AcquireCitusAdvisoryObjectClassLockGetOid(ObjectClass objectClass,
													 char *qualifiedObjectName);
static void AcquireCitusAdvisoryObjectClassLockCheckPrivileges(ObjectClass objectClass,
															   Oid oid);


/*
 * citus_internal_acquire_citus_advisory_object_class_lock is an internal UDF
 * to call AcquireCitusAdvisoryObjectClassLock().
 */
Datum
citus_internal_acquire_citus_advisory_object_class_lock(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	PG_ENSURE_ARGNOTNULL(0, "object_class");
	ObjectClass objectClass = PG_GETARG_INT32(0);

	char *qualifiedObjectName = PG_ARGISNULL(1) ? NULL : PG_GETARG_CSTRING(1);

	AcquireCitusAdvisoryObjectClassLock(objectClass, qualifiedObjectName);

	PG_RETURN_VOID();
}


/*
 * SerializeDistributedDDLsOnObjectClass is a wrapper around
 * SerializeDistributedDDLsOnObjectClassInternal to acquire the lock on given
 * object class itself, see the comment in header file for more details about
 * the difference between this function and
 * SerializeDistributedDDLsOnObjectClassObject().
 */
void
SerializeDistributedDDLsOnObjectClass(ObjectClass objectClass)
{
	SerializeDistributedDDLsOnObjectClassInternal(objectClass, NULL);
}


/*
 * SerializeDistributedDDLsOnObjectClassObject is a wrapper around
 * SerializeDistributedDDLsOnObjectClassInternal to acquire the lock on given
 * object that belongs to given object class, see the comment in header file
 * for more details about the difference between this function and
 * SerializeDistributedDDLsOnObjectClass().
 */
void
SerializeDistributedDDLsOnObjectClassObject(ObjectClass objectClass,
											char *qualifiedObjectName)
{
	if (qualifiedObjectName == NULL)
	{
		elog(ERROR, "qualified object name cannot be NULL");
	}

	SerializeDistributedDDLsOnObjectClassInternal(objectClass, qualifiedObjectName);
}


/*
 * SerializeDistributedDDLsOnObjectClassInternal serializes distributed DDLs
 * that target given object class by acquiring a Citus specific advisory lock
 * on the first primary worker node if there are any workers in the cluster.
 *
 * The lock is acquired via a coordinated transaction. For this reason,
 * it automatically gets released when (maybe implicit) transaction on
 * current server commits or rolls back.
 *
 * If qualifiedObjectName is provided to be non-null, then the oid of the
 * object is first resolved on the first primary worker node and then the
 * lock is acquired on that oid. If qualifiedObjectName is null, then the
 * lock is acquired on the object class itself.
 *
 * Note that those two lock types don't conflict with each other and are
 * acquired for different purposes. The lock on the object class
 * (qualifiedObjectName = NULL) is used to serialize DDLs that target the
 * object class itself, e.g., when creating a new object of that class, and
 * the latter is used to serialize DDLs that target a specific object of
 * that class, e.g., when altering an object.
 *
 * In some cases, we may want to acquire both locks at the same time. For
 * example, when renaming a database, we want to acquire both lock types
 * because while the object class lock is used to ensure that another session
 * doesn't create a new database with the same name, the object lock is used
 * to ensure that another session doesn't alter the same database.
 */
static void
SerializeDistributedDDLsOnObjectClassInternal(ObjectClass objectClass,
											  char *qualifiedObjectName)
{
	WorkerNode *firstWorkerNode = GetFirstPrimaryWorkerNode();
	if (firstWorkerNode == NULL)
	{
		/*
		 * If there are no worker nodes in the cluster, then we don't need
		 * to acquire the lock at all; and we cannot indeed.
		 */
		return;
	}

	/*
	 * Indeed we would already ensure permission checks in remote node
	 * --via AcquireCitusAdvisoryObjectClassLock()-- but we first do so on
	 * the local node to avoid from reporting confusing error messages.
	 */
	Oid oid = AcquireCitusAdvisoryObjectClassLockGetOid(objectClass, qualifiedObjectName);
	AcquireCitusAdvisoryObjectClassLockCheckPrivileges(objectClass, oid);

	Task *task = CitusMakeNode(Task);
	task->taskType = DDL_TASK;

	char *command = AcquireCitusAdvisoryObjectClassLockCommand(objectClass,
															   qualifiedObjectName);
	SetTaskQueryString(task, command);

	ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
	SetPlacementNodeMetadata(targetPlacement, firstWorkerNode);
	task->taskPlacementList = list_make1(targetPlacement);

	/* need to be in a transaction to acquire a lock that's bound to transactions */
	UseCoordinatedTransaction();

	bool localExecutionSupported = true;
	ExecuteUtilityTaskList(list_make1(task), localExecutionSupported);
}


/*
 * AcquireCitusAdvisoryObjectClassLockCommand returns a command to call
 * citus_internal.acquire_citus_advisory_object_class_lock().
 */
static char *
AcquireCitusAdvisoryObjectClassLockCommand(ObjectClass objectClass,
										   char *qualifiedObjectName)
{
	/* safe to cast to int as it's an enum */
	int objectClassInt = (int) objectClass;

	char *quotedObjectName =
		!qualifiedObjectName ? "NULL" :
		quote_literal_cstr(qualifiedObjectName);

	StringInfo command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT citus_internal.acquire_citus_advisory_object_class_lock(%d, %s)",
					 objectClassInt, quotedObjectName);

	return command->data;
}


/*
 * AcquireCitusAdvisoryObjectClassLock acquires a Citus specific advisory
 * ExclusiveLock based on given object class.
 */
static void
AcquireCitusAdvisoryObjectClassLock(ObjectClass objectClass, char *qualifiedObjectName)
{
	Oid oid = AcquireCitusAdvisoryObjectClassLockGetOid(objectClass, qualifiedObjectName);

	AcquireCitusAdvisoryObjectClassLockCheckPrivileges(objectClass, oid);

	LOCKTAG locktag;
	SET_LOCKTAG_GLOBAL_DDL_SERIALIZATION(locktag, objectClass, oid);

	LOCKMODE lockmode = ExclusiveLock;
	bool sessionLock = false;
	bool dontWait = false;
	LockAcquire(&locktag, lockmode, sessionLock, dontWait);
}


/*
 * AcquireCitusAdvisoryObjectClassLockGetOid returns the oid of given object
 * that belongs to given object class. If qualifiedObjectName is NULL, then
 * it returns InvalidOid.
 */
static Oid
AcquireCitusAdvisoryObjectClassLockGetOid(ObjectClass objectClass,
										  char *qualifiedObjectName)
{
	if (qualifiedObjectName == NULL)
	{
		return InvalidOid;
	}

	bool missingOk = false;

	switch (objectClass)
	{
		case OCLASS_DATABASE:
		{
			return get_database_oid(qualifiedObjectName, missingOk);
		}

		default:
			elog(ERROR, "unsupported object class: %d", objectClass);
	}
}


/*
 * AcquireCitusAdvisoryObjectClassLockCheckPrivileges is used to perform privilege checks
 * before acquiring the Citus specific advisory lock on given object class and oid.
 */
static void
AcquireCitusAdvisoryObjectClassLockCheckPrivileges(ObjectClass objectClass, Oid oid)
{
	switch (objectClass)
	{
		case OCLASS_DATABASE:
		{
			if (OidIsValid(oid) && !object_ownercheck(DatabaseRelationId, oid,
													  GetUserId()))
			{
				aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE,
							   get_database_name(oid));
			}
			else if (!OidIsValid(oid) && !have_createdb_privilege())
			{
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to create / rename database")));
			}

			break;
		}

		default:
			elog(ERROR, "unsupported object class: %d", objectClass);
	}
}
