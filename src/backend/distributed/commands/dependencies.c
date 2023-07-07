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

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"


static void EnsureDependenciesCanBeDistributed(const ObjectAddress *relationAddress);
static void ErrorIfCircularDependencyExists(const ObjectAddress *objectAddress);
static int ObjectAddressComparator(const void *a, const void *b);
static void EnsureDependenciesExistOnAllNodes(const ObjectAddress *target);
static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static bool ShouldPropagateObject(const ObjectAddress *address);
static char * DropTableIfExistsCommand(Oid relationId);


/*
 * Memory context and hash map for the distributed objects created in the current
 * transaction.
 */

/*
 * Memory context and data structure for storing distributed objects created in the
 * current transaction. We push stack for each subtransaction. We store hashmap of
 * distributed objects created in the subtransaction in each stack.
 */
static MemoryContext TxDistObjectsContext = NULL;
static List *TxDistObjects = NIL;


/*
 * InitDistObjectContext allocates memory context to track distributed objects
 * created in the current transaction.
 */
void
InitDistObjectContext(void)
{
	TxDistObjectsContext = AllocSetContextCreateInternal(TopMemoryContext,
														 "Tx Dist Object Context",
														 ALLOCSET_DEFAULT_MINSIZE,
														 ALLOCSET_DEFAULT_INITSIZE,
														 ALLOCSET_DEFAULT_MAXSIZE);
}


/*
 * PushDistObjectHash pushes a new hashmap to track distributed objects
 * created in the current subtransaction.
 */
void
PushDistObjectHash(void)
{
	HASHCTL info;
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ObjectAddress);
	info.entrysize = sizeof(ObjectAddress);
	info.hash = tag_hash;
	info.hcxt = TxDistObjectsContext;
	uint32 hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	HTAB *distObjectsHash = hash_create("citus tx dist objects hash",
										8, &info, hashFlags);

	MemoryContext oldContext = MemoryContextSwitchTo(TxDistObjectsContext);
	TxDistObjects = lappend(TxDistObjects, distObjectsHash);
	MemoryContextSwitchTo(oldContext);
}


/*
 * PushDistObjectHash removes the hashmap to track distributed objects
 * created in the current subtransaction.
 */
void
PopDistObjectHash(void)
{
	TxDistObjects = list_delete_last(TxDistObjects);
}


/*
 * AddToCurrentDistObjects adds given object into the distributed objects created in
 * the current subtransaction.
 */
void
AddToCurrentDistObjects(const ObjectAddress *objectAddress)
{
	if (TxDistObjects == NIL)
	{
		return;
	}

	HTAB *currentTxdistHash = (HTAB *) llast(TxDistObjects);
	hash_search(currentTxdistHash, objectAddress, HASH_ENTER, NULL);
}


/*
 * ResetDistObjects resets all distributed objects created in the current transaction.
 */
void
ResetDistObjects(void)
{
	HTAB *currentDistObjectHash = NULL;
	foreach_ptr(currentDistObjectHash, TxDistObjects)
	{
		hash_destroy(currentDistObjectHash);
	}
	list_free(TxDistObjects);
	TxDistObjects = NIL;
}


/*
 * AllDepsInDistObjects filters all dependencies which are created in the current
 * transaction.
 */
List *
AllDepsInDistObjects(List *dependencyList)
{
	List *currentTxDepList = NIL;
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencyList)
	{
		HTAB *distObjectsHash = NULL;
		foreach_ptr(distObjectsHash, TxDistObjects)
		{
			bool found = false;
			hash_search(distObjectsHash, dependency, HASH_FIND, &found);
			if (found)
			{
				currentTxDepList = lappend(currentTxDepList, dependency);
			}
		}
	}

	return currentTxDepList;
}


/*
 * RemoveDepsFromDistObjects removes given dependencies from TxDistObjects.
 */
void
RemoveDepsFromDistObjects(List *dependencyList)
{
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencyList)
	{
		HTAB *distObjectsHash = NULL;
		foreach_ptr(distObjectsHash, TxDistObjects)
		{
			hash_search(distObjectsHash, dependency, HASH_REMOVE, NULL);
		}
	}
}


/*
 * PropagateDistObjects propagated distributed objects created in the current transaction.
 */
void
PropagateDistObjects(void)
{
	HTAB *distObjectsHash = NULL;
	foreach_ptr(distObjectsHash, TxDistObjects)
	{
		HASH_SEQ_STATUS status;
		ObjectAddress *objectAddress = NULL;
		hash_seq_init(&status, distObjectsHash);

		while ((objectAddress = hash_seq_search(&status)) != NULL)
		{
			List *commands = GetDependencyCreateDDLCommands(objectAddress);
			SendCommandListToWorkersWithMetadata(commands);
		}
	}
}


/*
 * EnsureDependenciesExistOnAllNodes finds all the dependencies that we support and makes
 * sure these are available on all workers. If not available they will be created on the
 * workers via a separate session that will be committed directly so that the objects are
 * visible to potentially multiple sessions creating the shards.
 *
 * Note; only the actual objects are created via a separate session, the records to
 * pg_dist_object are created in this session. As a side effect the objects could be
 * created on the workers without a catalog entry. Updates to the objects on the coordinator
 * are not propagated to the workers until the record is visible on the coordinator.
 *
 * This is solved by creating the dependencies in an idempotent manner, either via
 * postgres native CREATE IF NOT EXISTS, or citus helper functions.
 */
static void
EnsureDependenciesExistOnAllNodes(const ObjectAddress *target)
{
	List *dependenciesWithCommands = NIL;

	/*
	 * If there is any unsupported dependency or circular dependency exists, Citus can
	 * not ensure dependencies will exist on all nodes.
	 */
	EnsureDependenciesCanBeDistributed(target);

	/* collect all dependencies in creation order and get their ddl commands */
	List *dependencies = GetDependenciesForObject(target);
	if (list_length(dependencies) <= 0)
	{
		/* no ddl commands to be executed */
		return;
	}

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by citus_add_node.
	 * This guarantees that all active nodes will have the object, because they will
	 * either get it now, or get it in citus_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(RowShareLock);

	/*
	 * Lock dependent objects explicitly to make sure same DDL command won't be sent
	 * multiple times from parallel sessions.
	 *
	 * Sort dependencies that will be created on workers to not to have any deadlock
	 * issue if different sessions are creating different objects.
	 */
	List *addressSortedDependencies = SortList(dependenciesWithCommands,
											   ObjectAddressComparator);
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, addressSortedDependencies)
	{
		LockDatabaseObject(dependency->classId, dependency->objectId,
						   dependency->objectSubId, ExclusiveLock);
	}

	/*
	 * We use super user outside connection to make sure deps, which are created before
	 * current transaction, can be propagated properly. Then, we use current user's
	 * metadata connection for dependencies which are created in the current transaction.
	 * We also remove current tx dependencies from `TxDistObjects` since they will be
	 * propagated by dependency creation. Otherwise, deferred propagation at COMMIT would
	 * try to recreate them.
	 */
	List *depsCreatedInCurrentTx = AllDepsInDistObjects(addressSortedDependencies);
	List *depsCreatedBeforeTx = list_difference(addressSortedDependencies,
												depsCreatedInCurrentTx);
	RemoveDepsFromDistObjects(depsCreatedInCurrentTx);

	/* since we are executing ddl commands lets disable propagation, primarily for mx */
	List *ddlTxCommands = list_make1(DISABLE_DDL_PROPAGATION);
	List *ddlBeforeTxCommands = list_make1(DISABLE_DDL_PROPAGATION);

	/* propagate deps created before current tx via superuser outside connection */
	if (depsCreatedBeforeTx)
	{
		ObjectAddress *beforeTxDep = NULL;
		foreach_ptr(beforeTxDep, depsCreatedBeforeTx)
		{
			List *dependencyCommands = GetDependencyCreateDDLCommands(dependency);
			ddlBeforeTxCommands = list_concat(ddlBeforeTxCommands, dependencyCommands);
		}

		WorkerNode *workerNode = NULL;
		foreach_ptr(workerNode, workerNodeList)
		{
			char *userName = CitusExtensionOwnerName();
			const char *nodeName = workerNode->workerName;
			uint32 nodePort = workerNode->workerPort;
			SendCommandListToWorkerOutsideTransaction(nodeName, nodePort,
													  userName, ddlBeforeTxCommands);
		}
	}

	/* propagate deps created in current tx via current user's metadata connection */
	if (depsCreatedInCurrentTx)
	{
		ObjectAddress *txDep = NULL;
		foreach_ptr(txDep, depsCreatedInCurrentTx)
		{
			List *dependencyCommands = GetDependencyCreateDDLCommands(dependency);
			ddlTxCommands = list_concat(ddlTxCommands, dependencyCommands);
		}

		ObjectAddress *firstDep = (ObjectAddress *) list_nth(depsCreatedInCurrentTx, 0);
		EnsureSequentialMode(get_object_type(firstDep->classId, firstDep->objectId));
		SendCommandListToWorkersWithMetadata(ddlTxCommands);
	}

	/*
	 * We do this after creating the objects on the workers, we make sure
	 * that objects have been created on worker nodes before marking them
	 * distributed, so MarkObjectDistributed wouldn't fail.
	 */
	foreach_ptr(dependency, dependenciesWithCommands)
	{
		/*
		 * pg_dist_object entries must be propagated with the super user, since
		 * the owner of the target object may not own dependencies but we must
		 * propagate as we send objects itself with the superuser.
		 *
		 * Only dependent object's metadata should be propagated with super user.
		 * Metadata of the table itself can be propagated with the current user.
		 */
		MarkObjectDistributedViaSuperUser(dependency);
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

			StringInfo detailInfo = makeStringInfo();
			appendStringInfo(detailInfo, "\"%s\" circularly depends itself, resolve "
										 "circular dependency first", objectDescription);

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
			List *databaseDDLCommands = NIL;

			/* only propagate the ownership of the database when the feature is on */
			if (EnableAlterDatabaseOwner)
			{
				List *ownerDDLCommands = DatabaseOwnerDDLCommands(dependency);
				databaseDDLCommands = list_concat(databaseDDLCommands, ownerDDLCommands);
			}

			return databaseDDLCommands;
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
