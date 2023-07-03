/*-------------------------------------------------------------------------
 *
 * common.c
 *
 *    Most of the object propagation code consists of mostly the same
 *    operations, varying slightly in parameters passed around. This
 *    file contains most of the reusable logic in object propagation.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"


/*
 * PostprocessCreateDistributedObjectFromCatalogStmt is a common function that can be used
 * for most objects during their creation phase. After the creation has happened locally
 * this function creates idempotent statements to recreate the object addressed by the
 * ObjectAddress of resolved from the creation statement.
 *
 * Since object already need to be able to create idempotent creation sql to support
 * scaleout operations we can reuse this logic during the initial creation of the objects
 * to reduce the complexity of implementation of new DDL commands.
 */
List *
PostprocessCreateDistributedObjectFromCatalogStmt(Node *stmt, const char *queryString)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	List *addresses = GetObjectAddressListFromParseTree(stmt, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	EnsureCoordinator();
	EnsureSequentialMode(ops->objectType);

	/* If the object has any unsupported dependency warn, and only create locally */
	DeferredErrorMessage *depError = DeferErrorIfAnyObjectHasUnsupportedDependency(
		addresses);
	if (depError != NULL)
	{
		if (EnableUnsupportedFeatureMessages)
		{
			RaiseDeferredError(depError, WARNING);
		}

		return NIL;
	}

	EnsureAllObjectDependenciesExistOnAllNodes(addresses);

	List *commands = GetAllDependencyCreateDDLCommands(addresses);

	commands = lcons(DISABLE_DDL_PROPAGATION, commands);
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterDistributedObjectStmt handles any updates to distributed objects by
 * creating the fully qualified sql to apply to all workers after checking all
 * predconditions that apply to propagating changes.
 *
 * Preconditions are (in order):
 *  - not in a CREATE/ALTER EXTENSION code block
 *  - citus.enable_metadata_sync is turned on
 *  - object being altered is distributed
 *  - any object specific feature flag is turned on when a feature flag is available
 *
 * Once we conclude to propagate the changes to the workers we make sure that the command
 * has been executed on the coordinator and force any ongoing transaction to run in
 * sequential mode. If any of these steps fail we raise an error to inform the user.
 *
 * Lastly we recreate a fully qualified version of the original sql and prepare the tasks
 * to send these sql commands to the workers. These tasks include instructions to prevent
 * recursion of propagation with Citus' MX functionality.
 */
List *
PreprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString,
									 ProcessUtilityContext processUtilityContext)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	List *addresses = GetObjectAddressListFromParseTree(stmt, false, false);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(ops->objectType);

	QualifyTreeNode(stmt);
	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDistributedObjectStmt is the counter part of
 * PreprocessAlterDistributedObjectStmt that should be executed after the object has been
 * changed locally.
 *
 * We perform the same precondition checks as before to skip this operation if any of the
 * failed during preprocessing. Since we already raised an error on other checks we don't
 * have to repeat them here, as they will never fail during postprocessing.
 *
 * When objects get altered they can start depending on undistributed objects. Now that
 * the objects has been changed locally we can find these new dependencies and make sure
 * they get created on the workers before we send the command list to the workers.
 */
List *
PostprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString)
{
	const DistributeObjectOps *ops = GetDistributeObjectOps(stmt);
	Assert(ops != NULL);

	List *addresses = GetObjectAddressListFromParseTree(stmt, false, true);

	/*  the code-path only supports a single object */
	Assert(list_length(addresses) == 1);

	if (!ShouldPropagateAnyObject(addresses))
	{
		return NIL;
	}

	if (ops->featureFlag && *ops->featureFlag == false)
	{
		/* not propagating when a configured feature flag is turned off by the user */
		return NIL;
	}

	EnsureAllObjectDependenciesExistOnAllNodes(addresses);

	return NIL;
}


/*
 * PreprocessDropDistributedObjectStmt is a general purpose hook that can propagate any
 * DROP statement.
 *
 * DROP statements are one of the few DDL statements that can work on many different
 * objects at once. Instead of resolving just one ObjectAddress and check it is
 * distributed we will need to lookup many different object addresses. Only if an object
 * was _not_ distributed we will need to remove it from the list of objects before we
 * recreate the sql statement.
 *
 * Given that we actually _do_ need to drop them locally we can't simply remove them from
 * the object list. Instead we create a new list where we only add distributed objects to.
 * Before we recreate the sql statement we put this list on the drop statement, so that
 * the SQL created will only contain the objects that are actually distributed in the
 * cluster. After we have the SQL we restore the old list so that all objects get deleted
 * locally.
 *
 * The reason we need to go through all this effort is taht we can't resolve the object
 * addresses anymore after the objects have been removed locally. Meaning during the
 * postprocessing we cannot understand which objects were distributed to begin with.
 */
List *
PreprocessDropDistributedObjectStmt(Node *node, const char *queryString,
									ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);

	/*
	 * We swap the list of objects to remove during deparse so we need a reference back to
	 * the old list to put back
	 */
	List *originalObjects = stmt->objects;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	QualifyTreeNode(node);

	List *distributedObjects = NIL;
	List *distributedObjectAddresses = NIL;
	Node *object = NULL;
	foreach_ptr(object, stmt->objects)
	{
		/* TODO understand if the lock should be sth else */
		Relation rel = NULL; /* not used, but required to pass to get_object_address */
		ObjectAddress address = get_object_address(stmt->removeType, object, &rel,
												   AccessShareLock, stmt->missing_ok);
		ObjectAddress *addressPtr = palloc0(sizeof(ObjectAddress));
		*addressPtr = address;
		if (IsAnyObjectDistributed(list_make1(addressPtr)))
		{
			distributedObjects = lappend(distributedObjects, object);
			distributedObjectAddresses = lappend(distributedObjectAddresses, addressPtr);
		}
	}

	if (list_length(distributedObjects) <= 0)
	{
		/* no distributed objects to drop */
		return NIL;
	}

	/*
	 * managing objects can only be done on the coordinator if ddl propagation is on. when
	 * it is off we will never get here. MX workers don't have a notion of distributed
	 * types, so we block the call.
	 */
	EnsureCoordinator();

	/*
	 * remove the entries for the distributed objects on dropping
	 */
	ObjectAddress *address = NULL;
	foreach_ptr(address, distributedObjectAddresses)
	{
		UnmarkObjectDistributed(address);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedObjects;
	char *dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = originalObjects;

	EnsureSequentialMode(stmt->removeType);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * DropTextSearchDictObjectAddress returns list of object addresses in
 * the drop tsdict statement.
 */
List *
DropTextSearchDictObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DropStmt *stmt = castNode(DropStmt, node);

	List *objectAddresses = NIL;

	List *objNameList = NIL;
	foreach_ptr(objNameList, stmt->objects)
	{
		Oid tsdictOid = get_ts_dict_oid(objNameList, missing_ok);

		ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*objectAddress, TSDictionaryRelationId, tsdictOid);
		objectAddresses = lappend(objectAddresses, objectAddress);
	}

	return objectAddresses;
}


/*
 * DropTextSearchConfigObjectAddress returns list of object addresses in
 * the drop tsconfig statement.
 */
List *
DropTextSearchConfigObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DropStmt *stmt = castNode(DropStmt, node);

	List *objectAddresses = NIL;

	List *objNameList = NIL;
	foreach_ptr(objNameList, stmt->objects)
	{
		Oid tsconfigOid = get_ts_config_oid(objNameList, missing_ok);

		ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*objectAddress, TSConfigRelationId, tsconfigOid);
		objectAddresses = lappend(objectAddresses, objectAddress);
	}

	return objectAddresses;
}
