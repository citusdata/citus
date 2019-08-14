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
#include "distributed/dist_catalog/distobjectaddress.h"

static List * GetDependencyCreateDDLCommands(const ObjectAddress *dependency);


/*
 * EnsureDependenciesExists finds all the dependencies that we can distribute and makes
 * sure these are available on all workers. If not available they will be created on the
 * workers via a separate session that will be committed directly so that the objects are
 * visible to potentially multiple sessions creating the shards.
 */
void
EnsureDependenciesExistsOnAllNodes(const ObjectAddress *target)
{
	const uint64 connectionFlag = FORCE_NEW_CONNECTION;
	ListCell *dependencyCell = NULL;

	List *dependencies = NIL;
	List *connections = NULL;
	ListCell *connectionCell = NULL;

	/* collect all dependencies in creation order and get their ddl commands */
	GetDependenciesForObject(target, &dependencies);

	/* create all dependencies on all nodes and mark them as distributed */
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		List *ddlCommands = GetDependencyCreateDDLCommands(dependency);

		if (list_length(ddlCommands) <= 0)
		{
			continue;
		}

		/* initialize connections on first commands to execute */
		if (connections == NULL)
		{
			/* first command to be executed connect to nodes */
			List *workerNodeList = ActivePrimaryNodeList();
			ListCell *workerNodeCell = NULL;

			if (list_length(workerNodeList) <= 0)
			{
				/* no nodes to execute on, we can break out */
				break;
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
		}

		/* create dependency on all worker nodes*/
		foreach(connectionCell, connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			ExecuteCriticalRemoteCommandList(connection, ddlCommands);
		}

		/* mark the object as distributed in this transaction */
		recordObjectDistributedByAddress(dependency);
	}

	foreach(connectionCell, connections)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		CloseConnection(connection);
	}
}


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


void
ReplicateAllDependenciesToNode(const char *nodeName, int nodePort)
{
	const uint64 connectionFlag = FORCE_NEW_CONNECTION;
	ListCell *dependencyCell = NULL;
	List *dependencies = NIL;
	List *ddlCommands = NIL;
	MultiConnection *connection = NULL;

	/* collect all dependencies in creation order and get their ddl commands */
	dependencies = GetDistributedObjectAddressList();
	dependencies = OrderObjectAddressListInDependencyOrder(dependencies);

	/* create all dependencies on all nodes and mark them as distributed */
	foreach(dependencyCell, dependencies)
	{
		ObjectAddress *dependency = (ObjectAddress *) lfirst(dependencyCell);
		ddlCommands = list_concat(ddlCommands,
								  GetDependencyCreateDDLCommands(dependency));
	}

	if (list_length(ddlCommands) <= 0)
	{
		/* no commands to replicate dependencies to the new worker*/
		return;
	}

	/* connect to the new host and create all applicable dependencies */
	connection = GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort,
											   CitusExtensionOwnerName(), NULL);
	ExecuteCriticalRemoteCommandList(connection, ddlCommands);
	CloseConnection(connection);
}
