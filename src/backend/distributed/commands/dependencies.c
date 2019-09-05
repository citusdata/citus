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
#include "storage/lmgr.h"

static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);


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
	const uint32 connectionFlag = FORCE_NEW_CONNECTION;

	/* local variables to work with dependencies */
	List *dependencies = NIL;
	ListCell *dependencyCell = NULL;

	/* local variables to collect ddl commands */
	List *ddlCommands = NULL;

	/* local variables to work with worker nodes */
	List *workerNodeList = NULL;
	ListCell *workerNodeCell = NULL;
	List *connections = NULL;
	ListCell *connectionCell = NULL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	dependencies = GetDependenciesForObject(target);
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		ddlCommands = list_concat(ddlCommands,
								  GetDependencyCreateDDLCommands(dependency));
	}
	if (list_length(ddlCommands) <= 0)
	{
		/* no ddl commands to be executed */
		return;
	}

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will have the object, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

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
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		MarkObjectDistributed(dependency);
	}

	/*
	 * collect and connect to all applicable nodes
	 */
	workerNodeList = ActivePrimaryNodeList();
	if (list_length(workerNodeList) <= 0)
	{
		/* no nodes to execute on */
		return;
	}
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		MultiConnection *connection = NULL;

		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;

		connection = StartNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
													 CitusExtensionOwnerName(), NULL);

		connections = lappend(connections, connection);
	}
	FinishConnectionListEstablishment(connections);

	/*
	 * create dependency on all nodes
	 */
	foreach(connectionCell, connections)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		ExecuteCriticalRemoteCommandList(connection, ddlCommands);
	}

	/*
	 * disconnect from nodes
	 */
	foreach(connectionCell, connections)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		CloseConnection(connection);
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
	return NIL;
}


/*
 * ReplicateAllDependenciesToNode replicate all previously marked objects to a worker node
 */
void
ReplicateAllDependenciesToNode(const char *nodeName, int nodePort)
{
	const uint32 connectionFlag = FORCE_NEW_CONNECTION;
	ListCell *dependencyCell = NULL;
	List *dependencies = NIL;
	List *ddlCommands = NIL;
	MultiConnection *connection = NULL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	dependencies = GetDistributedObjectAddressList();

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

	/*
	 * connect to the new host and create all applicable dependencies
	 */
	connection = GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
											   CitusExtensionOwnerName(), NULL);
	ExecuteCriticalRemoteCommandList(connection, ddlCommands);
	CloseConnection(connection);
}
