/*-------------------------------------------------------------------------
 *
 * foreign_server.c
 *    Commands for FOREIGN SERVER statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_foreign_server.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_transaction.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

static Node * RecreateForeignServerStmt(Oid serverId);
static bool NameListHasDistributedServer(List *serverNames);
static ObjectAddress GetObjectAddressByServerName(char *serverName, bool missing_ok);


/*
 * PreprocessCreateForeignServerStmt is called during the planning phase for
 * CREATE SERVER.
 */
List *
PreprocessCreateForeignServerStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_FOREIGN_SERVER);

	char *sql = DeparseTreeNode(node);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterForeignServerStmt is called during the planning phase for
 * ALTER SERVER .. OPTIONS ..
 */
List *
PreprocessAlterForeignServerStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext)
{
	AlterForeignServerStmt *stmt = castNode(AlterForeignServerStmt, node);

	ObjectAddress address = GetObjectAddressByServerName(stmt->servername, false);

	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();

	char *sql = DeparseTreeNode(node);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessRenameForeignServerStmt is called during the planning phase for
 * ALTER SERVER RENAME.
 */
List *
PreprocessRenameForeignServerStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_FOREIGN_SERVER);

	ObjectAddress address = GetObjectAddressByServerName(strVal(stmt->object), false);

	/* filter distributed servers */
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();

	char *sql = DeparseTreeNode(node);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterForeignServerOwnerStmt is called during the planning phase for
 * ALTER SERVER .. OWNER TO.
 */
List *
PreprocessAlterForeignServerOwnerStmt(Node *node, const char *queryString,
									  ProcessUtilityContext processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_FOREIGN_SERVER);

	ObjectAddress address = GetObjectAddressByServerName(strVal(stmt->object), false);

	/* filter distributed servers */
	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureCoordinator();

	char *sql = DeparseTreeNode(node);

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessDropForeignServerStmt is called during the planning phase for
 * DROP SERVER.
 */
List *
PreprocessDropForeignServerStmt(Node *node, const char *queryString,
								ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_FOREIGN_SERVER);

	bool includesDistributedServer = NameListHasDistributedServer(stmt->objects);

	if (!includesDistributedServer)
	{
		return NIL;
	}

	if (list_length(stmt->objects) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot drop distributed server with other servers"),
						errhint("Try dropping each object in a separate DROP command")));
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	Assert(list_length(stmt->objects) == 1);

	String *serverValue = linitial(stmt->objects);
	ObjectAddress address = GetObjectAddressByServerName(strVal(serverValue), false);

	/* unmark distributed server */
	UnmarkObjectDistributed(&address);

	const char *deparsedStmt = DeparseTreeNode((Node *) stmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) deparsedStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessCreateForeignServerStmt is called after a CREATE SERVER command has
 * been executed by standard process utility.
 */
List *
PostprocessCreateForeignServerStmt(Node *node, const char *queryString)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	const bool missingOk = false;
	ObjectAddress address = GetObjectAddressFromParseTree(node, missingOk);
	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


/*
 * PostprocessAlterForeignServerOwnerStmt is called after a ALTER SERVER OWNER command
 * has been executed by standard process utility.
 */
List *
PostprocessAlterForeignServerOwnerStmt(Node *node, const char *queryString)
{
	const bool missingOk = false;
	ObjectAddress address = GetObjectAddressFromParseTree(node, missingOk);

	if (!ShouldPropagateObject(&address))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	return NIL;
}


/*
 * CreateForeignServerStmtObjectAddress finds the ObjectAddress for the server
 * that is created by given CreateForeignServerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
CreateForeignServerStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateForeignServerStmt *stmt = castNode(CreateForeignServerStmt, node);

	return GetObjectAddressByServerName(stmt->servername, missing_ok);
}


/*
 * AlterForeignServerOwnerStmtObjectAddress finds the ObjectAddress for the server
 * given in AlterOwnerStmt. If missingOk is false and if
 * the server does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
AlterForeignServerOwnerStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	char *serverName = strVal(stmt->object);

	return GetObjectAddressByServerName(serverName, missing_ok);
}


/*
 * GetForeignServerCreateDDLCommand returns a list that includes the CREATE SERVER
 * command that would recreate the given server on a new node.
 */
List *
GetForeignServerCreateDDLCommand(Oid serverId)
{
	/* generate a statement for creation of the server in "if not exists" construct */
	Node *stmt = RecreateForeignServerStmt(serverId);

	/* capture ddl command for the create statement */
	const char *ddlCommand = DeparseTreeNode(stmt);

	List *ddlCommands = list_make1((void *) ddlCommand);

	return ddlCommands;
}


/*
 * RecreateForeignServerStmt returns a parsetree for a CREATE SERVER statement
 * that would recreate the given server on a new node.
 */
static Node *
RecreateForeignServerStmt(Oid serverId)
{
	ForeignServer *server = GetForeignServer(serverId);

	CreateForeignServerStmt *createStmt = makeNode(CreateForeignServerStmt);

	/* set server name and if_not_exists fields */
	createStmt->servername = pstrdup(server->servername);
	createStmt->if_not_exists = true;

	/* set foreign data wrapper */
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	createStmt->fdwname = pstrdup(fdw->fdwname);

	/* set all fields using the existing server */
	if (server->servertype != NULL)
	{
		createStmt->servertype = pstrdup(server->servertype);
	}

	if (server->serverversion != NULL)
	{
		createStmt->version = pstrdup(server->serverversion);
	}

	createStmt->options = NIL;

	int location = -1;
	DefElem *option = NULL;
	foreach_ptr(option, server->options)
	{
		DefElem *copyOption = makeDefElem(option->defname, option->arg, location);
		createStmt->options = lappend(createStmt->options, copyOption);
	}

	return (Node *) createStmt;
}


/*
 * NameListHasDistributedServer takes a namelist of servers and returns true if at least
 * one of them is distributed. Returns false otherwise.
 */
static bool
NameListHasDistributedServer(List *serverNames)
{
	String *serverValue = NULL;
	foreach_ptr(serverValue, serverNames)
	{
		ObjectAddress address = GetObjectAddressByServerName(strVal(serverValue), false);

		if (IsObjectDistributed(&address))
		{
			return true;
		}
	}

	return false;
}


static ObjectAddress
GetObjectAddressByServerName(char *serverName, bool missing_ok)
{
	ForeignServer *server = GetForeignServerByName(serverName, missing_ok);
	Oid serverOid = server->serverid;
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, ForeignServerRelationId, serverOid);

	return address;
}
