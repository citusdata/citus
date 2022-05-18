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
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

static void EnsureDependenciesCanBeDistributed(const ObjectAddress *relationAddress);
static void ErrorIfCircularDependencyExists(const ObjectAddress *objectAddress);
static int ObjectAddressComparator(const void *a, const void *b);
static List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate);

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
void
EnsureDependenciesExistOnAllNodes(const ObjectAddress *target)
{
	List *dependenciesWithCommands = NIL;
	List *ddlCommands = NULL;

	/*
	 * If there is any unsupported dependency or circular dependency exists, Citus can
	 * not ensure dependencies will exist on all nodes.
	 */
	EnsureDependenciesCanBeDistributed(target);

	/* collect all dependencies in creation order and get their ddl commands */
	List *dependencies = GetDependenciesForObject(target);
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		List *dependencyCommands = GetDependencyCreateDDLCommands(dependency);
		ddlCommands = list_concat(ddlCommands, dependencyCommands);

		/* create a new list with dependencies that actually created commands */
		if (list_length(dependencyCommands) > 0)
		{
			dependenciesWithCommands = lappend(dependenciesWithCommands, dependency);
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
	foreach_ptr(dependency, addressSortedDependencies)
	{
		LockDatabaseObject(dependency->classId, dependency->objectId,
						   dependency->objectSubId, ExclusiveLock);
	}

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		SendCommandListToWorkerOutsideTransaction(nodeName, nodePort,
												  CitusExtensionOwnerName(),
												  ddlCommands);
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
		 * Metadata of the table itself must be propagated with the current user.
		 */
		MarkObjectDistributedViaSuperUser(dependency);
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
	DeferredErrorMessage *depError = DeferErrorIfHasUnsupportedDependency(objectAddress);

	if (depError != NULL)
	{
		/* override error detail as it is not applicable here*/
		depError->detail = NULL;
		RaiseDeferredError(depError, ERROR);
	}
}


/*
 * ErrorIfCircularDependencyExists checks whether given object has circular dependency
 * with itself via existing objects of pg_dist_object.
 */
static void
ErrorIfCircularDependencyExists(const ObjectAddress *objectAddress)
{
	List *dependencies = GetAllSupportedDependenciesForObject(objectAddress);

	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		if (dependency->classId == objectAddress->classId &&
			dependency->objectId == objectAddress->objectId &&
			dependency->objectSubId == objectAddress->objectSubId)
		{
			char *objectDescription = NULL;

			#if PG_VERSION_NUM >= PG_VERSION_14
			objectDescription = getObjectDescription(objectAddress, false);
			#else
			objectDescription = getObjectDescription(objectAddress);
			#endif

			ereport(ERROR, (errmsg("Citus can not handle circular dependencies "
								   "between distributed objects"),
							errdetail("\"%s\" circularly depends itself, resolve "
									  "circular dependency first",
									  objectDescription)));
		}
	}
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
 * GetDependencyCreateDDLCommands returns a list (potentially empty or NIL) of ddl
 * commands to execute on a worker to create the object.
 */
List *
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
																		  creatingShellTableOnRemoteNode);
					TableDDLCommand *tableDDLCommand = NULL;
					foreach_ptr(tableDDLCommand, tableDDLCommands)
					{
						Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
						commandList = lappend(commandList, GetTableDDLCommand(
												  tableDDLCommand));
					}
				}

				return commandList;
			}

			if (relKind == RELKIND_SEQUENCE)
			{
				char *sequenceOwnerName = TableOwner(dependency->objectId);
				return DDLCommandsForSequence(dependency->objectId, sequenceOwnerName);
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
			return CreateFunctionDDLCommandsIdempotent(dependency);
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
			return GetForeignServerCreateDDLCommand(dependency->objectId);
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
						   getObjectTypeDescription_compat(dependency,

	                                                       /* missingOk: */ false)),
					errdetail(
						"citus tries to recreate an unsupported object on its workers"),
					errhint("please report a bug as this should not be happening")));
}


/*
 * ReplicateAllObjectsToNodeCommandList returns commands to replicate all
 * previously marked objects to a worker node. The function also sets
 * clusterHasDistributedFunction if there are any distributed functions.
 */
List *
ReplicateAllObjectsToNodeCommandList(const char *nodeName, int nodePort)
{
	/* since we are executing ddl commands disable propagation first, primarily for mx */
	List *ddlCommands = list_make1(DISABLE_DDL_PROPAGATION);

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	List *dependencies = GetDistributedObjectAddressList();

	/*
	 * Depending on changes in the environment, such as the enable_metadata_sync guc
	 * there might be objects in the distributed object address list that should currently
	 * not be propagated by citus as they are 'not supported'.
	 */
	dependencies = FilterObjectAddressListByPredicate(dependencies,
													  &SupportedDependencyByCitus);

	/*
	 * When dependency lists are getting longer we see a delay in the creation time on the
	 * workers. We would like to inform the user. Currently we warn for lists greater than
	 * 100 items, where 100 is an arbitrarily chosen number. If we find it too high or too
	 * low we can adjust this based on experience.
	 */
	if (list_length(dependencies) > 100)
	{
		ereport(NOTICE, (errmsg("Replicating postgres objects to node %s:%d", nodeName,
								nodePort),
						 errdetail("There are %d objects to replicate, depending on your "
								   "environment this might take a while",
								   list_length(dependencies))));
	}

	dependencies = OrderObjectAddressListInDependencyOrder(dependencies);
	ObjectAddress *dependency = NULL;
	foreach_ptr(dependency, dependencies)
	{
		if (IsObjectAddressOwnedByExtension(dependency, NULL))
		{
			/*
			 * we expect extension-owned objects to be created as a result
			 * of the extension being created.
			 */
			continue;
		}

		ddlCommands = list_concat(ddlCommands,
								  GetDependencyCreateDDLCommands(dependency));
	}

	ddlCommands = lappend(ddlCommands, ENABLE_DDL_PROPAGATION);

	return ddlCommands;
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
bool
ShouldPropagateObject(const ObjectAddress *address)
{
	if (!ShouldPropagate())
	{
		return false;
	}

	if (!IsObjectDistributed(address))
	{
		/* do not propagate for non-distributed types */
		return false;
	}

	return true;
}


/*
 * FilterObjectAddressListByPredicate takes a list of ObjectAddress *'s and returns a list
 * only containing the ObjectAddress *'s for which the predicate returned true.
 */
static List *
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
