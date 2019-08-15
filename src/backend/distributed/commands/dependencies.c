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

#include "distributed/dist_catalog/dependency.h"
#include "distributed/connection_management.h"
#include "distributed/worker_manager.h"
#include "distributed/metadata_sync.h"
#include "distributed/commands.h"
#include "distributed/remote_commands.h"
#include "distributed/dist_catalog/distobject.h"

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
	const uint64 connectionFlag = FORCE_NEW_CONNECTION;

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

		connection = GetNodeUserDatabaseConnection(connectionFlag, nodeName,
												   nodePort,
												   CitusExtensionOwnerName(),
												   NULL);

		connections = lappend(connections, connection);
	}

	/*
	 * create dependency on all nodes
	 */
	foreach(connectionCell, connections)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		ExecuteCriticalRemoteCommandList(connection, ddlCommands);
	}

	/*
	 * mark all objects as distributed
	 */
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		markObjectDistributed(dependency);
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
			return NIL;
		}
	}
}


/*
 * ReplicateAllDependenciesToNode replicate all previously marked objects to a worker node
 */
void
ReplicateAllDependenciesToNode(const char *nodeName, int nodePort)
{
	const uint64 connectionFlag = FORCE_NEW_CONNECTION;
	ListCell *dependencyCell = NULL;
	List *dependencies = NIL;
	List *ddlCommands = NIL;
	MultiConnection *connection = NULL;

	/*
	 * collect all dependencies in creation order and get their ddl commands
	 */
	dependencies = GetDistributedObjectAddressList();
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
