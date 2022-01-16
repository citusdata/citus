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
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

static int ObjectAddressComparator(const void *a, const void *b);
static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static List * GetCitusTableDDLCommandList(Oid relationId);
static List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate);

bool EnableDependencyCreation = true;

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
		MarkObjectDistributed(dependency);
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

			if (relKind == RELKIND_RELATION || relKind == RELKIND_PARTITIONED_TABLE ||
				relKind == RELKIND_FOREIGN_TABLE)
			{
				Oid relationId = dependency->objectId;
				List *commandList = NIL;

				if (IsCitusTable(relationId) && !IsTableOwnedByExtension(relationId))
				{
					commandList = GetCitusTableDDLCommandList(relationId);
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
 * GetCitusTableDDLCommandList returns the list of commands to create citus table
 * including the commands to associate sequences with table.
 */
static List *
GetCitusTableDDLCommandList(Oid relationId)
{
	List *commandList = NIL;
	List *tableDDLCommands = GetFullTableCreationCommands(relationId,
														  WORKER_NEXTVAL_SEQUENCE_DEFAULTS);

	TableDDLCommand *tableDDLCommand = NULL;
	foreach_ptr(tableDDLCommand, tableDDLCommands)
	{
		Assert(CitusIsA(tableDDLCommand, TableDDLCommand));
		commandList = lappend(commandList, GetTableDDLCommand(tableDDLCommand));
	}

	/*
	 * Get commands to associate sequences with dependencies
	 */
	List *sequenceDependencyCommandList = SequenceDependencyCommandList(relationId);
	commandList = list_concat(commandList, sequenceDependencyCommandList);

	return commandList;
}


/*
 * ReplicateAllDependenciesToNodeCommandList returns commands to replicate all
 * previously marked objects to a worker node. The function also sets
 * clusterHasDistributedFunction if there are any distributed functions.
 */
List *
ReplicateAllDependenciesToNodeCommandList(const char *nodeName, int nodePort)
{
	List *ddlCommands = NIL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	List *dependencies = GetDistributedObjectAddressList();

	/*
	 * Depending on changes in the environment, such as the enable_object_propagation guc
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
		ddlCommands = list_concat(ddlCommands,
								  GetDependencyCreateDDLCommands(dependency));
	}

	/* since we are executing ddl commands lets disable propagation, primarily for mx */
	ddlCommands = lcons(DISABLE_DDL_PROPAGATION, ddlCommands);
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

	if (!EnableDependencyCreation)
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
