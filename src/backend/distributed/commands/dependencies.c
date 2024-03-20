/*-------------------------------------------------------------------------
 *
 * dependencies.c
 *    Commands to create dependencies of an object on all workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "commands/extension.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

typedef enum RequiredObjectSet
{
	REQUIRE_ONLY_DEPENDENCIES = 1,
	REQUIRE_OBJECT_AND_DEPENDENCIES = 2,
} RequiredObjectSet;


static void EnsureDependenciesCanBeDistributed(const ObjectAddress *relationAddress);
static void ErrorIfCircularDependencyExists(const ObjectAddress *objectAddress);
static int ObjectAddressComparator(const void *a, const void *b);
static void EnsureDependenciesExistOnAllNodes(const ObjectAddress *target);
static void EnsureRequiredObjectSetExistOnAllNodes(const ObjectAddress *target,
												   RequiredObjectSet requiredObjectSet);
static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static bool ShouldPropagateObject(const ObjectAddress *address);
static char * DropTableIfExistsCommand(Oid relationId);

/*
 * EnsureObjectAndDependenciesExistOnAllNodes is a wrapper around
 * EnsureRequiredObjectSetExistOnAllNodes to ensure the "object itself" (together
 * with its dependencies) is available on all nodes.
 *
 * Different than EnsureDependenciesExistOnAllNodes, we return early if the
 * target object is distributed already.
 *
 * The reason why we don't do the same in EnsureDependenciesExistOnAllNodes
 * is that it's is used when altering an object too and hence the target object
 * may instantly have a dependency that needs to be propagated now. For example,
 * when "⁠GRANT non_dist_role TO dist_role" is executed, we need to propagate
 * "non_dist_role" to all nodes before propagating the "GRANT" command itself.
 * For this reason, we call EnsureDependenciesExistOnAllNodes for "dist_role"
 * and it would automatically discover that "non_dist_role" is a dependency of
 * "dist_role" and propagate it beforehand.
 *
 * However, when we're requested to create the target object itself (and
 * implicitly its dependencies), we're sure that we're not altering the target
 * object itself, hence we can return early if the target object is already
 * distributed. This is the case, for example, when
 * "REASSIGN OWNED BY dist_role TO non_dist_role" is executed. In that case,
 * "non_dist_role" is not a dependency of "dist_role" but we want to distribute
 * "non_dist_role" beforehand and we call this function for "non_dist_role",
 * not for "dist_role".
 *
 * See EnsureRequiredObjectExistOnAllNodes to learn more about how this
 * function deals with an object created within the same transaction.
 */
void
EnsureObjectAndDependenciesExistOnAllNodes(const ObjectAddress *target)
{
	if (IsAnyObjectDistributed(list_make1((ObjectAddress *) target)))
	{
		return;
	}
	EnsureRequiredObjectSetExistOnAllNodes(target, REQUIRE_OBJECT_AND_DEPENDENCIES);
}


/*
 * EnsureDependenciesExistOnAllNodes is a wrapper around
 * EnsureRequiredObjectSetExistOnAllNodes to ensure "all dependencies" of given
 * object --but not the object itself-- are available on all nodes.
 *
 * See EnsureRequiredObjectSetExistOnAllNodes to learn more about how this
 * function deals with an object created within the same transaction.
 */
static void
EnsureDependenciesExistOnAllNodes(const ObjectAddress *target)
{
	EnsureRequiredObjectSetExistOnAllNodes(target, REQUIRE_ONLY_DEPENDENCIES);
}


/*
 * EnsureRequiredObjectSetExistOnAllNodes finds all the dependencies that we support and makes
 * sure these are available on all nodes if required object set is REQUIRE_ONLY_DEPENDENCIES.
 * Otherwise, i.e., if required object set is REQUIRE_OBJECT_AND_DEPENDENCIES, then this
 * function creates the object itself on all nodes too. This function ensures that each
 * of the dependencies are supported by Citus but doesn't check the same for the target
 * object itself (when REQUIRE_OBJECT_AND_DEPENDENCIES) is provided because we assume that
 * callers don't call this function for an unsupported function at all.
 *
 * If not available, they will be created on the nodes via a separate session that will be
 * committed directly so that the objects are visible to potentially multiple sessions creating
 * the shards.
 *
 * Note; only the actual objects are created via a separate session, the records to
 * pg_dist_object are created in this session. As a side effect the objects could be
 * created on the nodes without a catalog entry. Updates to the objects on local node
 * are not propagated to the remote nodes until the record is visible on local node.
 *
 * This is solved by creating the dependencies in an idempotent manner, either via
 * postgres native CREATE IF NOT EXISTS, or citus helper functions.
 */
static void
EnsureRequiredObjectSetExistOnAllNodes(const ObjectAddress *target,
									   RequiredObjectSet requiredObjectSet)
{
	Assert(requiredObjectSet == REQUIRE_ONLY_DEPENDENCIES ||
		   requiredObjectSet == REQUIRE_OBJECT_AND_DEPENDENCIES);


	List *objectsWithCommands = NIL;
	List *ddlCommands = NULL;

	/*
	 * If there is any unsupported dependency or circular dependency exists, Citus can
	 * not ensure dependencies will exist on all nodes.
	 *
	 * Note that we don't check whether "target" is distributable (in case
	 * REQUIRE_OBJECT_AND_DEPENDENCIES is provided) because we expect callers
	 * to not even call this function if Citus doesn't know how to propagate
	 * "target" object itself.
	 */
	EnsureDependenciesCanBeDistributed(target);

	/* collect all dependencies in creation order and get their ddl commands */
	List *objectsToBeCreated = GetDependenciesForObject(target);

	/*
	 * Append the target object to make sure that it's created after its
	 * dependencies are created, if requested.
	 */
	if (requiredObjectSet == REQUIRE_OBJECT_AND_DEPENDENCIES)
	{
		ObjectAddress *targetCopy = palloc(sizeof(ObjectAddress));
		*targetCopy = *target;

		objectsToBeCreated = lappend(objectsToBeCreated, targetCopy);
	}

	ObjectAddress *object = NULL;
	foreach_ptr(object, objectsToBeCreated)
	{
		List *dependencyCommands = GetDependencyCreateDDLCommands(object);
		ddlCommands = list_concat(ddlCommands, dependencyCommands);

		/* create a new list with objects that actually created commands */
		if (list_length(dependencyCommands) > 0)
		{
			objectsWithCommands = lappend(objectsWithCommands, object);
		}
	}
	if (list_length(ddlCommands) <= 0)
	{
		/* no ddl commands to be executed */
		return;
	}

	/* since we are executing ddl commands lets disable propagation, primarily for mx */
	ddlCommands = list_concat(list_make1(DISABLE_DDL_PROPAGATION), ddlCommands);

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by citus_add_node.
	 * This guarantees that all active nodes will have the object, because they will
	 * either get it now, or get it in citus_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	List *remoteNodeList = ActivePrimaryRemoteNodeList(RowShareLock);

	/*
	 * Lock objects to be created explicitly to make sure same DDL command won't be sent
	 * multiple times from parallel sessions.
	 *
	 * Sort the objects that will be created on workers to not to have any deadlock
	 * issue if different sessions are creating different objects.
	 */
	List *addressSortedDependencies = SortList(objectsWithCommands,
											   ObjectAddressComparator);
	foreach_ptr(object, addressSortedDependencies)
	{
		LockDatabaseObject(object->classId, object->objectId,
						   object->objectSubId, ExclusiveLock);
	}


	/*
	 * We need to propagate objects via the current user's metadata connection if
	 * any of the objects that we're interested in are created in the current transaction.
	 * Our assumption is that if we rely on an object created in the current transaction,
	 * then the current user, most probably, has permissions to create the target object
	 * as well.
	 *
	 * Note that, user still may not be able to create the target due to no permissions
	 * for any of its dependencies. But this is ok since it should be rare.
	 *
	 * If we opted to use a separate superuser connection for the target, then we would
	 * have visibility issues since propagated dependencies would be invisible to
	 * the separate connection until we locally commit.
	 */
	List *createdObjectList = GetAllSupportedDependenciesForObject(target);

	/* consider target as well if we're requested to create it too */
	if (requiredObjectSet == REQUIRE_OBJECT_AND_DEPENDENCIES)
	{
		ObjectAddress *targetCopy = palloc(sizeof(ObjectAddress));
		*targetCopy = *target;

		createdObjectList = lappend(createdObjectList, targetCopy);
	}

	if (HasAnyObjectInPropagatedObjects(createdObjectList))
	{
		SendCommandListToRemoteNodesWithMetadata(ddlCommands);
	}
	else
	{
		WorkerNode *workerNode = NULL;
		foreach_ptr(workerNode, remoteNodeList)
		{
			const char *nodeName = workerNode->workerName;
			uint32 nodePort = workerNode->workerPort;

			SendCommandListToWorkerOutsideTransaction(nodeName, nodePort,
													  CitusExtensionOwnerName(),
													  ddlCommands);
		}
	}

	/*
	 * We do this after creating the objects on remote nodes, we make sure
	 * that objects have been created on remote nodes before marking them
	 * distributed, so MarkObjectDistributed wouldn't fail.
	 */
	foreach_ptr(object, objectsWithCommands)
	{
		/*
		 * pg_dist_object entries must be propagated with the super user, since
		 * the owner of the target object may not own dependencies but we must
		 * propagate as we send objects itself with the superuser.
		 *
		 * Only dependent object's metadata should be propagated with super user.
		 * Metadata of the table itself must be propagated with the current user.
		 */
		MarkObjectDistributedViaSuperUser(object);
	}
}


/*
 * EnsureAllObjectDependenciesExistOnAllNodes iteratively calls EnsureDependenciesExistOnAllNodes
 * for given targets.
 */
void
EnsureAllObjectDependenciesExistOnAllNodes(const List *targets)
{
	ObjectAddress *target = NULL;
	foreach_ptr(target, targets)
	{
		EnsureDependenciesExistOnAllNodes(target);
	}
}


/*
 * EnsureDependenciesCanBeDistributed ensures all dependencies of the given object
 * can be distributed.
 */
static void
EnsureDependenciesCanBeDistributed(const ObjectAddress *objectAddress)
{
	/* If the object circularcly depends to itself, Citus can not handle it */
	ErrorIfCircularDependencyExists(objectAddress);

	/* If the object has any unsupported dependency, error out */
	DeferredErrorMessage *depError = DeferErrorIfAnyObjectHasUnsupportedDependency(
		list_make1((ObjectAddress *) objectAddress));

	if (depError != NULL)
	{
		/* override error detail as it is not applicable here*/
		depError->detail = NULL;
		RaiseDeferredError(depError, ERROR);
	}
}


/*
 * ErrorIfCircularDependencyExists is a wrapper around
 * DeferErrorIfCircularDependencyExists(), and throws error
 * if circular dependency exists.
 */
static void
ErrorIfCircularDependencyExists(const ObjectAddress *objectAddress)
{
	DeferredErrorMessage *depError =
		DeferErrorIfCircularDependencyExists(objectAddress);
	if (depError != NULL)
	{
		RaiseDeferredError(depError, ERROR);
	}
}


/*
 * DeferErrorIfCircularDependencyExists checks whether given object has
 * circular dependency with itself. If so, returns a deferred error.
 */
DeferredErrorMessage *
DeferErrorIfCircularDependencyExists(const ObjectAddress *objectAddress)
{
	List *dependencies = GetAllDependenciesForObject(objectAddress);

	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		if (dependency->classId == objectAddress->classId &&
			dependency->objectId == objectAddress->objectId &&
			dependency->objectSubId == objectAddress->objectSubId)
		{
			char *objectDescription = getObjectDescription(objectAddress, false);

			// Iterate all objects in the 'dependencies' list, get the object names and create a string
			ListCell *cell;
			StringInfo objectNames = makeStringInfo();
			foreach(cell, dependencies)
			{
				ObjectAddress *dependency1 = (ObjectAddress *) lfirst(cell);
				char *objectName = getObjectDescription(dependency1, false);
				appendStringInfo(objectNames, "%s\n", objectName);
			}
			//to show the circular dependency





			StringInfo detailInfo = makeStringInfo();
			appendStringInfo(detailInfo, "\"%s\" circularly depends itself, resolve "
										 "circular dependency first", objectDescription);
			appendStringInfo(detailInfo, "ependencies are:\n%s", objectNames->data);

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Citus can not handle circular dependencies "
								 "between distributed objects", detailInfo->data,
								 NULL);
		}
	}

	return NULL;
}


/*
 * Copied from PG object_address_comparator function to compare ObjectAddresses.
 */
static int
ObjectAddressComparator(const void *a, const void *b)
{
	const ObjectAddress *obja = (const ObjectAddress *) a;
	const ObjectAddress *objb = (const ObjectAddress *) b;

	/*
	 * Primary sort key is OID descending.
	 */
	if (obja->objectId > objb->objectId)
	{
		return -1;
	}
	if (obja->objectId < objb->objectId)
	{
		return 1;
	}

	/*
	 * Next sort on catalog ID, in case identical OIDs appear in different
	 * catalogs. Sort direction is pretty arbitrary here.
	 */
	if (obja->classId < objb->classId)
	{
		return -1;
	}
	if (obja->classId > objb->classId)
	{
		return 1;
	}

	/*
	 * Last, sort on object subId.
	 */
	if ((unsigned int) obja->objectSubId < (unsigned int) objb->objectSubId)
	{
		return -1;
	}
	if ((unsigned int) obja->objectSubId > (unsigned int) objb->objectSubId)
	{
		return 1;
	}
	return 0;
}


/*
 * GetDistributableDependenciesForObject finds all the dependencies that Citus
 * can distribute and returns those dependencies regardless of their existency
 * on nodes.
 */
List *
GetDistributableDependenciesForObject(const ObjectAddress *target)
{
	/* local variables to work with dependencies */
	List *distributableDependencies = NIL;

	/* collect all dependencies in creation order */
	List *dependencies = GetDependenciesForObject(target);

	/* filter the ones that can be distributed */
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		/*
		 * TODO: maybe we can optimize the logic applied in below line. Actually we
		 * do not need to create ddl commands as we are not ensuring their existence
		 * in nodes, but we utilize logic it follows to choose the objects that could
		 * be distributed
		 */
		List *dependencyCommands = GetDependencyCreateDDLCommands(dependency);

		/* create a new list with dependencies that actually created commands */
		if (list_length(dependencyCommands) > 0)
		{
			distributableDependencies = lappend(distributableDependencies, dependency);
		}
	}

	return distributableDependencies;
}


/*
 * DropTableIfExistsCommand returns command to drop given table if exists.
 */
static char *
DropTableIfExistsCommand(Oid relationId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	StringInfo dropTableCommand = makeStringInfo();
	appendStringInfo(dropTableCommand, "DROP TABLE IF EXISTS %s CASCADE",
					 qualifiedRelationName);

	return dropTableCommand->data;
}


/*
 * GetDependencyCreateDDLCommands returns a list (potentially empty or NIL) of ddl
 * commands to execute on a worker to create the object.
 */
static List *
GetDependencyCreateDDLCommands(const ObjectAddress *dependency)
{
	switch (getObjectClass(dependency))
	{
		case OCLASS_CLASS:
		{
			char relKind = get_rel_relkind(dependency->objectId);

			/*
			 * types have an intermediate dependency on a relation (aka class), so we do
			 * support classes when the relkind is composite
			 */
			if (relKind == RELKIND_COMPOSITE_TYPE)
			{
				return NIL;
			}

			/*
			 * Indices are created separately, however, they do show up in the dependency
			 * list for a table since they will have potentially their own dependencies.
			 * The commands will be added to both shards and metadata tables via the table
			 * creation commands.
			 */
			if (relKind == RELKIND_INDEX ||
				relKind == RELKIND_PARTITIONED_INDEX)
			{
				return NIL;
			}

			if (relKind == RELKIND_RELATION || relKind == RELKIND_PARTITIONED_TABLE ||
				relKind == RELKIND_FOREIGN_TABLE)
			{
				Oid relationId = dependency->objectId;
				List *commandList = NIL;

				if (IsCitusTable(relationId))
				{
					bool creatingShellTableOnRemoteNode = true;
					List *tableDDLCommands = GetFullTableCreationCommands(relationId,
																		  WORKER_NEXTVAL_SEQUENCE_DEFAULTS,
																		  INCLUDE_IDENTITY,
																		  creatingShellTableOnRemoteNode);
					TableDDLCommand *tableDDLCommand = NULL;
					foreach_ptr(tableDDLCommand, tableDDLCommands)
					{
						Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
						commandList = lappend(commandList, GetTableDDLCommand(
												  tableDDLCommand));
					}

					/*
					 * We need to drop table, if exists, first to make table creation
					 * idempotent. Before dropping the table, we should also break
					 * dependencies with sequences since `drop cascade table` would also
					 * drop depended sequences. This is safe as we still record dependency
					 * with the sequence during table creation.
					 */
					commandList = lcons(DropTableIfExistsCommand(relationId),
										commandList);
					commandList = lcons(WorkerDropSequenceDependencyCommand(relationId),
										commandList);
				}

				return commandList;
			}

			if (relKind == RELKIND_SEQUENCE)
			{
				char *sequenceOwnerName = TableOwner(dependency->objectId);
				return DDLCommandsForSequence(dependency->objectId, sequenceOwnerName);
			}

			if (relKind == RELKIND_VIEW)
			{
				char *createViewCommand = CreateViewDDLCommand(dependency->objectId);
				char *alterViewOwnerCommand = AlterViewOwnerCommand(dependency->objectId);

				return list_make2(createViewCommand, alterViewOwnerCommand);
			}

			/* if this relation is not supported, break to the error at the end */
			break;
		}

		case OCLASS_COLLATION:
		{
			return CreateCollationDDLsIdempotent(dependency->objectId);
		}

		case OCLASS_CONSTRAINT:
		{
			/*
			 * Constraints can only be reached by domains, they resolve functions.
			 * Constraints themself are recreated by the domain recreation.
			 */
			return NIL;
		}

		case OCLASS_DATABASE:
		{
			/*
			 * For the database where Citus is installed, only propagate the ownership of the
			 * database, only when the feature is on.
			 *
			 * This is because this database must exist on all nodes already so we shouldn't
			 * need to "CREATE" it on other nodes. However, we still need to correctly reflect
			 * its owner on other nodes too.
			 */
			if (dependency->objectId == MyDatabaseId && EnableAlterDatabaseOwner)
			{
				return DatabaseOwnerDDLCommands(dependency);
			}

			/*
			 * For the other databases, create the database on all nodes, only when the feature
			 * is on.
			 */
			if (dependency->objectId != MyDatabaseId && EnableCreateDatabasePropagation)
			{
				return GetDatabaseMetadataSyncCommands(dependency->objectId);
			}

			return NIL;
		}

		case OCLASS_PROC:
		{
			List *DDLCommands = CreateFunctionDDLCommandsIdempotent(dependency);
			List *grantDDLCommands = GrantOnFunctionDDLCommands(dependency->objectId);
			DDLCommands = list_concat(DDLCommands, grantDDLCommands);
			return DDLCommands;
		}

		case OCLASS_PUBLICATION:
		{
			return CreatePublicationDDLCommandsIdempotent(dependency);
		}

		case OCLASS_ROLE:
		{
			return GenerateCreateOrAlterRoleCommand(dependency->objectId);
		}

		case OCLASS_SCHEMA:
		{
			char *schemaDDLCommand = CreateSchemaDDLCommand(dependency->objectId);

			List *DDLCommands = list_make1(schemaDDLCommand);

			List *grantDDLCommands = GrantOnSchemaDDLCommands(dependency->objectId);

			DDLCommands = list_concat(DDLCommands, grantDDLCommands);

			return DDLCommands;
		}

		case OCLASS_TSCONFIG:
		{
			return CreateTextSearchConfigDDLCommandsIdempotent(dependency);
		}

		case OCLASS_TSDICT:
		{
			return CreateTextSearchDictDDLCommandsIdempotent(dependency);
		}

		case OCLASS_TYPE:
		{
			return CreateTypeDDLCommandsIdempotent(dependency);
		}

		case OCLASS_EXTENSION:
		{
			return CreateExtensionDDLCommand(dependency);
		}

		case OCLASS_FOREIGN_SERVER:
		{
			Oid serverId = dependency->objectId;

			List *DDLCommands = GetForeignServerCreateDDLCommand(serverId);
			List *grantDDLCommands = GrantOnForeignServerDDLCommands(serverId);
			DDLCommands = list_concat(DDLCommands, grantDDLCommands);

			return DDLCommands;
		}

		default:
		{
			break;
		}
	}

	/*
	 * make sure it fails hard when in debug mode, leave a hint for the user if this ever
	 * happens in production
	 */
	Assert(false);
	ereport(ERROR, (errmsg("unsupported object %s for distribution by citus",
						   getObjectTypeDescription(dependency,

	                                                /* missingOk: */ false)),
					errdetail(
						"citus tries to recreate an unsupported object on its workers"),
					errhint("please report a bug as this should not be happening")));
}


/*
 * GetAllDependencyCreateDDLCommands iteratively calls GetDependencyCreateDDLCommands
 * for given dependencies.
 */
List *
GetAllDependencyCreateDDLCommands(const List *dependencies)
{
	List *commands = NIL;

	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		commands = list_concat(commands, GetDependencyCreateDDLCommands(dependency));
	}

	return commands;
}


/*
 * ShouldPropagate determines if we should be propagating anything
 */
bool
ShouldPropagate(void)
{
	if (creating_extension)
	{
		/*
		 * extensions should be created separately on the workers, types cascading from an
		 * extension should therefore not be propagated.
		 */
		return false;
	}

	if (!EnableMetadataSync)
	{
		/*
		 * we are configured to disable object propagation, should not propagate anything
		 */
		return false;
	}

	return true;
}


/*
 * ShouldPropagateCreateInCoordinatedTransction returns based the current state of the
 * session and policies if Citus needs to propagate the creation of new objects.
 *
 * Creation of objects on other nodes could be postponed till the object is actually used
 * in a sharded object (eg. distributed table or index on a distributed table). In certain
 * use cases the opportunity for parallelism in a transaction block is preferred. When
 * configured like that the creation of an object might be postponed and backfilled till
 * the object is actually used.
 */
bool
ShouldPropagateCreateInCoordinatedTransction()
{
	if (!IsMultiStatementTransaction())
	{
		/*
		 * If we are in a single statement transaction we will always propagate the
		 * creation of objects. There are no downsides in regard to performance or
		 * transactional limitations. These only arise with transaction blocks consisting
		 * of multiple statements.
		 */
		return true;
	}

	if (MultiShardConnectionType == SEQUENTIAL_CONNECTION)
	{
		/*
		 * If we are in a transaction that is already switched to sequential, either by
		 * the user, or automatically by an other command, we will always propagate the
		 * creation of new objects to the workers.
		 *
		 * This guarantees no strange anomalies when the transaction aborts or on
		 * visibility of the newly created object.
		 */
		return true;
	}

	switch (CreateObjectPropagationMode)
	{
		case CREATE_OBJECT_PROPAGATION_DEFERRED:
		{
			/*
			 * We prefer parallelism at this point. Since we did not already return while
			 * checking for sequential mode we are still in parallel mode. We don't want
			 * to switch that now, thus not propagating the creation.
			 */
			return false;
		}

		case CREATE_OBJECT_PROPAGATION_AUTOMATIC:
		{
			/*
			 * When we run in optimistic mode we want to switch to sequential mode, only
			 * if this would _not_ give an error to the user. Meaning, we either are
			 * already in sequential mode (checked earlier), or there has been no parallel
			 * execution in the current transaction block.
			 *
			 * If switching to sequential would throw an error we would stay in parallel
			 * mode while creating new objects. We will rely on Citus' mechanism to ensure
			 * the existence if the object would be used in the same transaction.
			 */
			if (ParallelQueryExecutedInTransaction())
			{
				return false;
			}

			return true;
		}

		case CREATE_OBJECT_PROPAGATION_IMMEDIATE:
		{
			return true;
		}

		default:
		{
			elog(ERROR, "unsupported ddl propagation mode");
		}
	}
}


/*
 * ShouldPropagateObject determines if we should be propagating DDLs based
 * on their object address.
 */
static bool
ShouldPropagateObject(const ObjectAddress *address)
{
	if (!ShouldPropagate())
	{
		return false;
	}

	if (!IsAnyObjectDistributed(list_make1((ObjectAddress *) address)))
	{
		/* do not propagate for non-distributed types */
		return false;
	}

	return true;
}


/*
 * ShouldPropagateAnyObject determines if we should be propagating DDLs based
 * on their object addresses.
 */
bool
ShouldPropagateAnyObject(List *addresses)
{
	ObjectAddress *address = NULL;
	foreach_ptr(address, addresses)
	{
		if (ShouldPropagateObject(address))
		{
			return true;
		}
	}

	return false;
}


/*
 * FilterObjectAddressListByPredicate takes a list of ObjectAddress *'s and returns a list
 * only containing the ObjectAddress *'s for which the predicate returned true.
 */
List *
FilterObjectAddressListByPredicate(List *objectAddressList, AddressPredicate predicate)
{
	List *result = NIL;

	ObjectAddress *address = NULL;
	foreach_ptr(address, objectAddressList)
	{
		if (predicate(address))
		{
			result = lappend(result, address);
		}
	}

	return result;
}
