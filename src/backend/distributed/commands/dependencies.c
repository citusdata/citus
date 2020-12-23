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
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"

typedef bool (*AddressPredicate)(const ObjectAddress *);

static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 AddressPredicate predicate);

bool EnableDependencyCreation = true;

/*
 * EnsureDependenciesExistOnAllNodes finds all the dependencies that we support and makes
 * sure these are available on all workers. If not available they will be created on the
 * workers via a separate session that will be committed directly so that the objects are
 * visible to potentially multiple sessions creating the shards.
 *
 * Note; only the actual objects are created via a separate session, the local records to
 * pg_dist_object are created in this session. As a side effect the objects could be
 * created on the workers without a catalog entry on the coordinator. Updates to the
 * objects on the coordinator are not propagated to the workers until the record is
 * visible on the coordinator.
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
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will have the object, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	List *workerNodeList = ActivePrimaryNonCoordinatorNodeList(RowShareLock);

	/*
	 * right after we acquired the lock we mark our objects as distributed, these changes
	 * will not become visible before we have successfully created all the objects on our
	 * workers.
	 *
	 * It is possible to create distributed tables which depend on other dependencies
	 * before any node is in the cluster. If we would wait till we actually had connected
	 * to the nodes before marking the objects as distributed these objects would never be
	 * created on the workers when they get added, causing shards to fail to create.
	 */
	foreach_ptr(dependency, dependenciesWithCommands)
	{
		MarkObjectDistributed(dependency);
	}

	/*
	 * collect and connect to all applicable nodes
	 */
	if (list_length(workerNodeList) <= 0)
	{
		/* no nodes to execute on */
		return;
	}


	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		SendCommandListToWorkerInSingleTransaction(nodeName, nodePort,
												   CitusExtensionOwnerName(),
												   ddlCommands);
	}
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
			/*
			 * types have an intermediate dependency on a relation (aka class), so we do
			 * support classes when the relkind is composite
			 */
			if (get_rel_relkind(dependency->objectId) == RELKIND_COMPOSITE_TYPE)
			{
				return NIL;
			}

			/* if this relation is not supported, break to the error at the end */
			break;
		}

		case OCLASS_COLLATION:
		{
			return CreateCollationDDLsIdempotent(dependency->objectId);
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
						   getObjectTypeDescription(dependency)),
					errdetail(
						"citus tries to recreate an unsupported object on its workers"),
					errhint("please report a bug as this should not be happening")));
}


/*
 * ReplicateAllDependenciesToNode replicate all previously marked objects to a worker
 * node. The function also sets clusterHasDistributedFunction if there are any
 * distributed functions.
 */
void
ReplicateAllDependenciesToNode(const char *nodeName, int nodePort)
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
	if (list_length(ddlCommands) <= 0)
	{
		/* no commands to replicate dependencies to the new worker */
		return;
	}

	/* since we are executing ddl commands lets disable propagation, primarily for mx */
	ddlCommands = list_concat(list_make1(DISABLE_DDL_PROPAGATION), ddlCommands);

	SendCommandListToWorkerInSingleTransaction(nodeName, nodePort,
											   CitusExtensionOwnerName(), ddlCommands);
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
