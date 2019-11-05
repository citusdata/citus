/*-------------------------------------------------------------------------
 *
 * dependencies.c
 *    Commands to create dependencies of an object on all workers.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"

static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);
static List * FilterObjectAddressListByPredicate(List *objectAddressList,
												 bool (*predicate)(const
																   ObjectAddress *));

bool EnableDependencyCreation = true;

/*
 * EnsureDependenciesExists finds all the dependencies that we support and makes sure
 * these are available on all workers. If not available they will be created on the
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
EnsureDependenciesExistsOnAllNodes(const ObjectAddress *target)
{
	/* local variables to work with dependencies */
	List *dependencies = NIL;
	List *dependenciesWithCommands = NIL;
	ListCell *dependencyCell = NULL;

	/* local variables to collect ddl commands */
	List *ddlCommands = NULL;

	/* local variables to work with worker nodes */
	List *workerNodeList = NULL;
	ListCell *workerNodeCell = NULL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	dependencies = GetDependenciesForObject(target);
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
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
	workerNodeList = ActivePrimaryWorkerNodeList(RowShareLock);

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
	foreach(dependencyCell, dependenciesWithCommands)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
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


	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		const char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		SendCommandListToWorkerInSingleTransaction(nodeName, nodePort,
												   CitusExtensionOwnerName(),
												   ddlCommands);
	}
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

		case OCLASS_PROC:
		{
			return CreateFunctionDDLCommandsIdempotent(dependency);
		}

		case OCLASS_SCHEMA:
		{
			const char *schemaDDLCommand = CreateSchemaDDLCommand(dependency->objectId);

			if (schemaDDLCommand == NULL)
			{
				/* no schema to create */
				return NIL;
			}

			return list_make1((void *) schemaDDLCommand);
		}

		case OCLASS_TYPE:
		{
			return CreateTypeDDLCommandsIdempotent(dependency);
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
	ListCell *dependencyCell = NULL;
	List *dependencies = NIL;
	List *ddlCommands = NIL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	dependencies = GetDistributedObjectAddressList();

	/*
	 * Depending on changes in the environment, such as the enable_object_propagation guc
	 * there might be objects in the distributed object address list that should currently
	 * not be propagated by citus as the are 'not supported'.
	 */
	dependencies = FilterObjectAddressListByPredicate(dependencies,
													  &SupportedDependencyByCitus);

	/*
	 * When dependency lists are getting longer we see a delay in the creation time on the
	 * workers. We would like to inform the user. Currently we warn for lists greater then
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
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
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
 * FilterObjectAddressListByPredicate takes a list of ObjectAddress *'s and returns a list
 * only containing the ObjectAddress *'s for which the predicate returned true.
 */
static List *
FilterObjectAddressListByPredicate(List *objectAddressList,
								   bool (*predicate)(const ObjectAddress *))
{
	List *result = NIL;
	ListCell *objectAddressListCell = NULL;

	foreach(objectAddressListCell, objectAddressList)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(objectAddressListCell);
		if (predicate(address))
		{
			result = lappend(result, address);
		}
	}

	return result;
}
